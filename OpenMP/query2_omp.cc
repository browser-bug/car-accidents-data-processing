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

#include "../utilities/CSVIterator.h"

#define DEBUG 0

// Dataset
#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

// Query2
#define NUM_CONTRIBUTING_FACTORS 47
#define MAX_CF_PER_ROW 5
#define MAX_CF_LENGTH 55

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
    int indexCF = 0;
    map<string, int> cfDictionary;

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

    AccPair local_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {}; // Local data structure for QUERY2

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

        vector<string> cfs = row.getContributingFactors();

        for (unsigned int k = 0; k < cfs.size(); k++)
        {
            // Populating dictionary for QUERY2
            if (cfDictionary.find(cfs[k]) == cfDictionary.end())
                cfDictionary.insert({cfs[k], indexCF++});
        }
    }

    loadDuration = cpuSecond() - loadBegin;

    // [2] Data processing
    procBegin = cpuSecond();

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
#pragma omp parallel for shared(local_accAndPerc)
    for (unsigned int i = 0; i < localRows.size(); i++)
    {
        CSVRow row = localRows[i];

        int cfIndex = 0; // used for indexing the two dictionaries
        int lethal = (row.getNumPersonsKilled() > 0) ? 1 : 0;
        vector<string> cfs = row.getContributingFactors();

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

        /* Query2 */
        for (unsigned int k = 0; k < cfs.size(); k++)
        {
            cfIndex = cfDictionary.at(cfs[k]);
#pragma omp atomic
            (local_accAndPerc[cfIndex].numAccidents)++;
#pragma omp atomic
            (local_accAndPerc[cfIndex].numLethalAccidents) += lethal;
        }
    }

    procDuration = cpuSecond() - procBegin;

    // [3] Output results
    writeBegin = cpuSecond();

    // Print Query2 results
    cout << "********* QUERY 2 *********" << endl;
    for (auto el : cfDictionary)
    {
        double perc = double(local_accAndPerc[el.second].numLethalAccidents) / local_accAndPerc[el.second].numAccidents;
        cout << el.first << endl
             << "\t\tNum. of accidents: " << local_accAndPerc[el.second].numAccidents
             << "\t\t\t\t\tPerc. lethal accidents: " << setprecision(4) << fixed << perc * 100 << "%"
             << endl;
    }
    cout << "Total CF parsed: " << cfDictionary.size() << "\n\n\n";

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
