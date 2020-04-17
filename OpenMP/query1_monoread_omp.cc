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

#include "../utilities/csv_row/include/CSVIterator.h"

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
    if (argc != 3)
    {
        cout << "Usage: " << argv[0] << " <num_omp_threads> <dataset_dimension>" << endl;
        exit(-1);
    }
    int num_omp_threads = stoi(argv[1]);
    string dataset_dim = argv[2];

    bool testing = false; // switch between dataset for testing and original dataset
    // int err;              // used for MPI error messages

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

    int local_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {}; // Local data structure for QUERY1

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
    }
    my_num_rows = localRows.size();

    loadDuration = cpuSecond() - loadBegin;

    // [2] Data processing
    procBegin = cpuSecond();

    int dynChunk = (int)round(my_num_rows * 0.02); // this tunes the chunk size exploited by dynamic scheduling based on percentage
    cout << "Started processing dataset with " << dynChunk << " dynamic chunk size..." << endl;
    omp_set_num_threads(num_omp_threads);

#pragma omp parallel for default(shared) schedule(dynamic, dynChunk) reduction(+ \
                                                                               : local_lethAccPerWeek)
    for (int i = 0; i < my_num_rows; i++)
    {
        int lethal = (localRows[i].getNumPersonsKilled() > 0) ? 1 : 0;
        int week, month, year = 0;

        if (lethal)
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

            /* Query1 */
            local_lethAccPerWeek[year][week]++;
        }
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
