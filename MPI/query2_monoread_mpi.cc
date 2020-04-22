#include <mpi.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <string>
#include <string.h> // for string copy
#include <cstddef>

#include "utils/csv_row/CSVIterator.h"

#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

#define NUM_CONTRIBUTING_FACTORS 47

#define MAX_CF_PER_ROW 5
#define MAX_CF_LENGTH 55

// Data structure representing a row used for pre-processing
typedef struct Row
{
    Row() : num_pers_killed(0), num_contributing_factors(0){};
    Row(int npk,
        int ncf)
        : num_pers_killed(npk), num_contributing_factors(ncf){};

    int num_pers_killed;

    char contributing_factors[MAX_CF_PER_ROW][MAX_CF_LENGTH] = {};
    int num_contributing_factors;
} Row;

typedef struct AccPair
{
    AccPair() : numAccidents(0), numLethalAccidents(0){};
    AccPair(int na,
            int nla) : numAccidents(na), numLethalAccidents(nla){};

    int numAccidents;
    int numLethalAccidents;
} AccPair;

/* We implement a user-defind operation to manage the summation of AccPair */
void pairSum(void *inputBuffer, void *outputBuffer, int *len, MPI_Datatype *dptr)
{
    AccPair *in = (AccPair *)inputBuffer;
    AccPair *inout = (AccPair *)outputBuffer;

    for (int i = 0; i < *len; ++i)
    {
        inout[i].numAccidents += in[i].numAccidents;
        inout[i].numLethalAccidents += in[i].numLethalAccidents;
    }
}

using namespace std;

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        cout << "Usage: " << argv[0] << " <dataset_dimension>" << endl;
        exit(-1);
    }
    string dataset_dim = argv[1];
    // int err;              // used for MPI error messages

    // Load dataset variables
    const string dataset_path = "../dataset/";
    const string csv_path = dataset_path + "collisions_" + dataset_dim + ".csv";
    int csv_size = 0;

    // Support dictonaries
    int indexCF = 0;
    map<string, int> cfDictionary;
    char(*cfKeys)[MAX_CF_LENGTH]; // this contains all cf keys in the dataset
    int *cfValues;                // this contains all cf values in the dataset
    int num_cf;

    // MPI variables
    int myrank, num_workers;
    double *overallTimes; // these contain time stats for each process (e.g. overallTimes[3] -> overall duration time of rank = 3 process)
    double *loadTimes;
    double *scatterTimes;
    double *procTimes;
    double *writeTimes;

    AccPair global_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};

    // MPI Datatypes definitions
    MPI_Datatype rowType;
    int rowLength[] = {
        1,
        MAX_CF_PER_ROW * MAX_CF_LENGTH,
        1};
    MPI_Aint rowDisplacements[] = {
        offsetof(Row, num_pers_killed),
        offsetof(Row, contributing_factors),
        offsetof(Row, num_contributing_factors)};
    MPI_Datatype rowTypes[] = {MPI_INT, MPI_CHAR, MPI_INT};

    // MPI Operators definitions
    MPI_Op accPairSum;

    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);

    MPI_Type_create_struct(3, rowLength, rowDisplacements, rowTypes, &rowType);
    MPI_Type_commit(&rowType);

    MPI_Op_create(pairSum, true, &accPairSum);

    // Local data structures
    vector<Row> dataScatter;
    vector<Row> localRows;
    int scatterCount[num_workers];
    int dataDispl[num_workers];
    int my_num_rows;
    int my_row_displ;

    // Timing stats variables
    double overallBegin, overallDuration; // overall application duration time
    double loadBegin, loadDuration;       // loading phase duration time
    double scatterBegin, scatterDuration; // scattering phase duration time
    double procBegin, procDuration;       // processing phase duration time
    double writeBegin, writeDuration;     // printing stats duration time

    AccPair local_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};

    // Initialization for timing statistics
    if (myrank == 0)
    {
        overallTimes = new double[num_workers]();
        loadTimes = new double[num_workers]();
        scatterTimes = new double[num_workers]();
        procTimes = new double[num_workers]();
        writeTimes = new double[num_workers]();
    }

    overallBegin = MPI_Wtime();

    // [1] Loading data from file
    loadBegin = MPI_Wtime();

    if (myrank == 0)
    {
        cout << "[Proc. " + to_string(myrank) + "] Started loading dataset..." << endl;
        ifstream file(csv_path);
        CSVRow row;
        for (CSVIterator loop(file); loop != CSVIterator(); ++loop)
        {
            if (!(*loop)[TIME].compare("TIME")) // TODO: find a nicer way to skip the header
                continue;

            row = (*loop);

            Row newRow(row.getNumPersonsKilled(), 0);

            vector<string> cfs = row.getContributingFactors();
            for (unsigned int k = 0; k < cfs.size(); k++)
            {
                strcpy(newRow.contributing_factors[k], cfs[k].c_str());
                newRow.num_contributing_factors++;

                // Populating dictonary for QUERY2
                if (cfDictionary.find(cfs[k]) == cfDictionary.end())
                    cfDictionary.insert({cfs[k], indexCF++});
            }

            dataScatter.push_back(newRow);
        }
        csv_size = dataScatter.size();
    }

    loadDuration = MPI_Wtime() - loadBegin;
    MPI_Gather(&loadDuration, 1, MPI_DOUBLE, loadTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD); /* wait for master to complete reading */

    // Initialization for scattering, evenly dividing dataset
    scatterBegin = MPI_Wtime();
    MPI_Bcast(&csv_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (myrank == 0)
    {
        scatterCount[myrank] = csv_size / num_workers + csv_size % num_workers; // TODO maybe change this so master process doesn't get overloaded with too many rows
        my_num_rows = scatterCount[myrank];

        dataDispl[myrank] = 0;
        my_row_displ = 0;

        int datasetOffset = 0;
        for (int rank = 1; rank < num_workers; rank++)
        {
            scatterCount[rank] = csv_size / num_workers;
            datasetOffset += scatterCount[rank - 1];
            dataDispl[rank] = datasetOffset;

            MPI_Send(&scatterCount[rank], 1, MPI_INT, rank, 13, MPI_COMM_WORLD);
            MPI_Send(&dataDispl[rank], 1, MPI_INT, rank, 14, MPI_COMM_WORLD);
        }
    }
    else
    {
        MPI_Recv(&my_num_rows, 1, MPI_INT, 0, 13, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&my_row_displ, 1, MPI_INT, 0, 14, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    // Broadcasting the dictionaries to all processes
    if (myrank == 0)
        num_cf = cfDictionary.size();
    MPI_Bcast(&num_cf, 1, MPI_INT, 0, MPI_COMM_WORLD);

    cfKeys = (char(*)[MAX_CF_LENGTH])malloc(num_cf * sizeof(*cfKeys));
    cfValues = new int[num_cf];
    if (myrank == 0)
    {
        int i = 0;
        for (auto cf : cfDictionary)
        {
            strncpy(cfKeys[i], cf.first.c_str(), MAX_CF_LENGTH);
            cfValues[i] = cf.second;
            i++;
        }
    }

    MPI_Bcast(cfKeys, num_cf * MAX_CF_LENGTH, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast(cfValues, num_cf, MPI_INT, 0, MPI_COMM_WORLD);

    if (myrank != 0)
    {
        for (int i = 0; i < num_cf; i++)
            cfDictionary.insert({cfKeys[i], cfValues[i]});
    }

    // Scattering data to all workers
    localRows.resize(my_num_rows);
    MPI_Scatterv(dataScatter.data(), scatterCount, dataDispl, rowType, localRows.data(), my_num_rows, rowType, 0, MPI_COMM_WORLD);

    scatterDuration = MPI_Wtime() - scatterBegin;
    MPI_Gather(&scatterDuration, 1, MPI_DOUBLE, scatterTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // [2] Data processing
    procBegin = MPI_Wtime();

    cout << "[Proc. " + to_string(myrank) + "] Started processing dataset..." << endl;
    int cfIndex;
    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
    for (int i = 0; i < my_num_rows; i++)
    {
        // cout << "WorkerID: " << myrank << " num CF. " << localRows[i].num_contributing_factors << endl;
        int lethal = (localRows[i].num_pers_killed > 0) ? 1 : 0;

        for (int k = 0; k < localRows[i].num_contributing_factors; k++)
        {
            cfIndex = cfDictionary.at(string(localRows[i].contributing_factors[k]));
            (local_accAndPerc[cfIndex].numAccidents)++;
            (local_accAndPerc[cfIndex].numLethalAccidents) += lethal;
        }
    }

    MPI_Reduce(local_accAndPerc, global_accAndPerc, NUM_CONTRIBUTING_FACTORS, MPI_2INT, accPairSum, 0, MPI_COMM_WORLD);

    procDuration = MPI_Wtime() - procBegin;
    MPI_Gather(&procDuration, 1, MPI_DOUBLE, procTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // [3] Output results
    writeBegin = MPI_Wtime();

    if (myrank == 0)
    {
        // Print Query2 results
        cout << "********* QUERY 2 *********" << endl;
        for (auto el : cfDictionary)
        {
            double perc = double(global_accAndPerc[el.second].numLethalAccidents) / global_accAndPerc[el.second].numAccidents;
            cout << el.first << endl
                 << "\t\tNum. of accidents: " << global_accAndPerc[el.second].numAccidents
                 << "\t\t\t\t\tPerc. lethal accidents: " << setprecision(4) << fixed << perc * 100 << "%"
                 << endl;
        }
        cout << "Total CF parsed: " << cfDictionary.size() << "\n\n\n";
    }

    writeDuration = MPI_Wtime() - writeBegin;
    MPI_Gather(&writeDuration, 1, MPI_DOUBLE, writeTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    overallDuration = MPI_Wtime() - overallBegin;
    MPI_Gather(&overallDuration, 1, MPI_DOUBLE, overallTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // Print statistics
    if (myrank == 0)
    {
        cout << fixed << setprecision(8) << endl;
        cout << "********* Timing Statistics *********" << endl;
        cout << "NUM. MPI PROCESSES:\t" << num_workers << endl;

        double avgOverall, avgLoading, avgScattering, avgProcessing, avgWriting = 0;
        for (int i = 0; i < num_workers; i++)
        {
            cout << "Process {" << i << "}\t";
            if (i == 0)
                cout << "Loading(" << loadTimes[i] << "), ";
            if (i != 0)
            {
                cout << "Scattering(" << scatterTimes[i] << "), ";
                avgScattering += scatterTimes[i];
            }
            cout << "Processing(" << procTimes[i] << "), ";
            if (i == 0)
                cout << "Writing(" << writeTimes[i] << "), ";
            cout << "took overall " << overallTimes[i] << " seconds" << endl;

            avgOverall += overallTimes[i];
            avgLoading += loadTimes[i];
            avgProcessing += procTimes[i];
            avgWriting += writeTimes[i];
        }
        cout << endl
             << "With average times of: "
             << "[1] Loading(" << loadTimes[0] << "), "
             << "[1a] Scattering(" << (num_workers > 1 ? avgScattering / (num_workers - 1) : 0) << "), "
             << "[2] Processing(" << avgProcessing / num_workers << "), "
             << "[3] Writing(" << writeTimes[0] << ") and \t"
             << "overall(" << avgOverall / num_workers << ").\n";
    }

    delete[] cfKeys;
    delete[] cfValues;

    if (myrank == 0)
    {
        delete[] overallTimes;
        delete[] loadTimes;
        delete[] procTimes;
        delete[] writeTimes;
    }

    MPI_Finalize();
}
