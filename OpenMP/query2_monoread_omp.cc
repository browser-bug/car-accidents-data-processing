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
    int indexCF = 0;
    map<string, int> cfDictionary;

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

    AccPair local_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {}; // Local data structure for QUERY2

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

        vector<string> cfs = row.getContributingFactors();

        for (unsigned int k = 0; k < cfs.size(); k++)
        {
            // Populating dictionary for QUERY2
            if (cfDictionary.find(cfs[k]) == cfDictionary.end())
                cfDictionary.insert({cfs[k], indexCF++});
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
                                                                               : local_accAndPerc)
    for (int i = 0; i < my_num_rows; i++)
    {
        int cfIndex = 0; // used for indexing the two dictionaries
        int lethal = (localRows[i].getNumPersonsKilled() > 0) ? 1 : 0;
        vector<string> cfs = localRows[i].getContributingFactors();

        /* Query2 */
        for (unsigned int k = 0; k < cfs.size(); k++)
        {
            cfIndex = cfDictionary.at(cfs[k]);
            (local_accAndPerc[cfIndex].numAccidents)++;
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
