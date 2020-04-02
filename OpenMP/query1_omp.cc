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

// Query1
#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

using namespace std;

int main(int argc, char **argv)
{
    int num_omp_threads = 4; // TODO this will be assigned by user input

    bool testing = false; // switch between dataset for testing and original dataset
    // int err;              // used for MPI error messages

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

    int local_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {}; // Local data structure for QUERY1

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
    }

    loadDuration = cpuSecond() - loadBegin;

    // [2] Data processing
    procBegin = cpuSecond();

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
#pragma omp parallel for shared(local_lethAccPerWeek)
    for (unsigned int i = 0; i < localRows.size(); i++)
    {
        CSVRow row = localRows[i];

        int lethal = (row.getNumPersonsKilled() > 0) ? 1 : 0;

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

        /* Query1 */
        if (lethal)
#pragma omp atomic
            local_lethAccPerWeek[year][week]++;
    }

    procDuration = cpuSecond() - procBegin;

    // [3] Output results
    writeBegin = cpuSecond();

    // Print Query1 results
    cout << "********* QUERY 1 *********" << endl;
    int totalWeeks = 0;
    int totalAccidents = 0;

    for (int year = 0; year < NUM_YEARS; year++)
    {
        for (int week = 0; week < NUM_WEEKS_PER_YEAR; week++)
        {
            int numLethAcc = local_lethAccPerWeek[year][week];
            if (numLethAcc > 0)
            {
                cout << "(" << (year + 2012) << ")Week: " << (week + 1) << "\t\t\t Num. lethal accidents: ";
                cout << numLethAcc << endl;
                totalAccidents += numLethAcc;
                totalWeeks++;
            }
        }
    }
    cout << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << "\n\n\n";

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
