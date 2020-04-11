#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <string>
#include <string.h> // for string copy
#include <unistd.h> // for debugging
#include <cstddef>

#include "../utilities/csv_row/CSVIterator.h"

#define DEBUG 0

// Dataset
#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

// Query3
#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

#define NUM_BOROUGH 5
#define MAX_BOROUGH_LENGTH 15

typedef struct AccPair
{
    AccPair() : numAccidents(0), numLethalAccidents(0){};
    AccPair(int na,
            int nla) : numAccidents(na), numLethalAccidents(nla){};

    int numAccidents;
    int numLethalAccidents;
} AccPair;

using namespace std;

int main(int argc, char **argv)
{
    int num_omp_threads = 4; // TODO this will be assigned by user input

    bool testing = false; // switch between dataset for testing and original dataset
    // int err;              // used for MPI error messages

    // Support dictonaries
    int indexBrgh = 0;
    map<string, int> brghDictionary;

    // Load dataset variables
    // TODO : maybe the csv_size can be specified at runtime by user
    int csv_size = testing ? TEST_SIZE : ORIGINAL_SIZE;
    // csv_size = 29996; // Set the first N rows to be read
    const string dataset_path = "../dataset/";
    const string csv_path = testing ? dataset_path + "data_test.csv" : dataset_path + "collisions_1M.csv";

    vector<CSVRow> localRows;

    // Timing stats variables
    double overallBegin, overallDuration; // overall application duration time
    double loadBegin, loadDuration;       // loading phase duration time
    double procBegin, procDuration;       // processing phase duration time
    double writeBegin, writeDuration;     // printing stats duration time

    AccPair local_boroughWeekAcc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {}; // Local data structure for QUERY3

    overallBegin = cpuSecond();

    // [1] Loading data from file
    loadBegin = cpuSecond();

    ifstream file(csv_path);
    CSVRow row;
    for (int i = 0; i < csv_size; i++)
    {
        if (i == 0)
            file >> row >> row; // skip the header
        else
            file >> row;

        localRows.push_back(row);

        string borough = row[BOROUGH];

        if (!borough.empty())
        {
            // Populating dictionary for QUERY3
            if (brghDictionary.find(borough) == brghDictionary.end())
                brghDictionary.insert({borough, indexBrgh++});
        }
    }

    loadDuration = cpuSecond() - loadBegin;

    // [2] Data processing
    procBegin = cpuSecond();

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
#pragma omp parallel for shared(local_boroughWeekAcc)
    for (unsigned int i = 0; i < localRows.size(); i++)
    {
        CSVRow row = localRows[i];

        int brghIndex = 0; // used for indexing the two dictionaries
        int lethal = (row.getNumPersonsKilled() > 0) ? 1 : 0;
        string borough = row[BOROUGH];

        int week = getWeek(row[DATE]);
        int month = getMonth(row[DATE]);
        int year = getYear(row[DATE]);

        // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
        // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
        if (week == 1 && month == 12)
            year++;
        else if ((week == 52 || week == 53) && month == 1)
            year--;

        year = year - 2012;
        week = week - 1;

        /* Query3 */
        if (!borough.empty()) // if borough is not specified we're not interested
        {
            brghIndex = brghDictionary.at(borough);
#pragma omp atomic
            local_boroughWeekAcc[brghIndex][year][week].numAccidents++;
#pragma omp atomic
            local_boroughWeekAcc[brghIndex][year][week].numLethalAccidents += lethal;
        }
    }

    procDuration = cpuSecond() - procBegin;

    // [3] Output results
    writeBegin = cpuSecond();

    // Print Query3 results
    cout << "********* QUERY 3 *********" << endl;
    for (auto b : brghDictionary)
    {
        int numWeeks = 0;
        int numAccidents = 0;
        double numLethalAccidents = 0;

        cout << "Borough: " << b.first << endl;
        for (int i = 0; i < NUM_YEARS; i++) // for each year
        {
            for (int j = 0; j < NUM_WEEKS_PER_YEAR; j++) // for each week
            {
                if (local_boroughWeekAcc[b.second][i][j].numAccidents == 0)
                    continue;
                numWeeks++;
                numAccidents += local_boroughWeekAcc[b.second][i][j].numAccidents;
                numLethalAccidents += local_boroughWeekAcc[b.second][i][j].numLethalAccidents;

                cout << "(" << (i + 2012) << ")Week " << (j + 1);                                               // print (Year)Week N
                cout << "\t\t\t num. accidents: " << local_boroughWeekAcc[b.second][i][j].numAccidents << endl; // print numAccidents
            }
        }
        double avg = numLethalAccidents / numWeeks;
        cout << "[" << b.first << "] Avg. lethal accidents per week is: " << setprecision(2) << fixed << avg * 100 << "%";
        cout << "\t\t\tNum. accidents: " << numAccidents << "\t\tNum. lethal accidents: " << setprecision(0) << fixed << numLethalAccidents << endl;
        cout << endl;
    }
    cout << "Total boroughs parsed: " << brghDictionary.size() << "\n\n\n";

    writeDuration = cpuSecond() - writeBegin;

    overallDuration = cpuSecond() - overallBegin;

    // Print statistics
    cout << fixed << setprecision(8) << endl;
    cout << "********* Timing Statistics *********" << endl;
    cout << "NUM. OMP THREADS:\t" << num_omp_threads << endl;

    cout << "Loading(" << loadDuration << "), ";
    cout << "Processing(" << procDuration << "), ";
    cout << "Writing(" << writeDuration << "), ";
    cout << "took overall " << overallDuration << " seconds" << endl;
}
