#include <omp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <math.h>
#include <string>
#include <string.h> // for string copy
#include <unistd.h> // for debugging
#include <cstddef>

#include "utils/csv_row/CSVIterator.h"

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

AccPair &operator+=(AccPair &out, const AccPair &in)
{
    out.numAccidents += in.numAccidents;
    out.numLethalAccidents += in.numLethalAccidents;
    return out;
}

using namespace std;

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        cout << "Usage: " << argv[0] << " <num_omp_threads> <dataset_dimension>" << endl;
        exit(-1);
    }
    int num_omp_threads = stoi(argv[1]);
    string dataset_dim = argv[2];

    bool testing = false; // switch between dataset for testing and original dataset
    // int err;              // used for MPI error messages

    // Support dictonaries
    int indexBrgh = 0;
    map<string, int> brghDictionary;

    // Load dataset variables
    const string dataset_path = "../dataset/";
    const string csv_path = testing ? dataset_path + "data_test.csv" : dataset_path + "collisions_" + dataset_dim + ".csv";
    int my_num_rows;

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

    cout << "Started loading dataset... " << endl;
    ifstream file(csv_path);
    CSVRow row;
    for (CSVIterator loop(file); loop != CSVIterator(); ++loop)
    {
        if (!(*loop)[TIME].compare("TIME")) // TODO: find a nicer way to skip the header
            continue;

        row = (*loop);
        localRows.push_back(row);

        string borough = row[BOROUGH];

        if (!borough.empty())
        {
            // Populating dictionary for QUERY3
            if (brghDictionary.find(borough) == brghDictionary.end())
                brghDictionary.insert({borough, indexBrgh++});
        }
    }
    my_num_rows = localRows.size();

    loadDuration = cpuSecond() - loadBegin;

    // [2] Data processing
    procBegin = cpuSecond();

    int dynChunk = (int)round(my_num_rows * 0.02); // this tunes the chunk size exploited by dynamic scheduling based on percentage
    cout << "Started processing dataset with " << dynChunk << " dynamic chunk size..." << endl;
    omp_set_num_threads(num_omp_threads);
#pragma omp declare reduction(pairSum:AccPair \
                              : omp_out += omp_in)

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
#pragma omp parallel for default(shared) schedule(dynamic, dynChunk) reduction(pairSum \
                                                                               : local_boroughWeekAcc)
    for (int i = 0; i < my_num_rows; i++)
    {
        int brghIndex = 0; // used for indexing the two dictionaries
        int week, month, year = 0;
        int lethal = (localRows[i].getNumPersonsKilled() > 0) ? 1 : 0;
        string borough = localRows[i][BOROUGH];

        if (!borough.empty())
        {
            week = getWeek(localRows[i][DATE]);
            month = getMonth(localRows[i][DATE]);
            year = getYear(localRows[i][DATE]);

            // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
            // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
            if (week == 1 && month == 12)
                year++;
            else if ((week == 52 || week == 53) && month == 1)
                year--;

            year = year - 2012;
            week = week - 1;

            /* Query3 */
            brghIndex = brghDictionary.at(borough);
            local_boroughWeekAcc[brghIndex][year][week].numAccidents++;
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
