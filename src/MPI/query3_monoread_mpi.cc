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
#include <unistd.h> // for debugging
#include <cstddef>

#include "utils/csv_row/CSVIterator.h"

// Dataset
#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

#define DATE_LENGTH 11

#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

#define NUM_BOROUGH 5
#define MAX_BOROUGH_LENGTH 15

// Data structure representing a row used for pre-processing
typedef struct Row
{
    Row() : num_pers_killed(0){};
    Row(int npk)
        : num_pers_killed(npk){};

    char date[MAX_DATE_LENGTH] = {};

    int num_pers_killed;

    char borough[MAX_BOROUGH_LENGTH] = {};
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

    // Load dataset variables
    const string dataset_path = "../dataset/";
    const string csv_path = dataset_path + "collisions_" + dataset_dim + ".csv";
    int csv_size = 0;

    // Support dictionaries
    int indexBrgh = 0;
    map<string, int> brghDictionary;
    char(*brghKeys)[MAX_BOROUGH_LENGTH]; // this contains all borough keys in the dataset
    int *brghValues;                     // this contains all borough values in the dataset
    int num_brgh;

    // MPI variables
    int myrank, num_workers;
    double *overallTimes; // these contain time stats for each process (e.g. overallTimes[3] -> overall duration time of rank = 3 process)
    double *loadTimes;
    double *scatterTimes;
    double *procTimes;
    double *writeTimes;

    AccPair global_boroughWeekAc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    // MPI Datatypes definitions
    MPI_Datatype rowType;
    int rowLength[] = {
        MAX_DATE_LENGTH,
        1,
        MAX_BOROUGH_LENGTH};
    MPI_Aint rowDisplacements[] = {
        offsetof(Row, date),
        offsetof(Row, num_pers_killed),
        offsetof(Row, borough)};
    MPI_Datatype rowTypes[] = {MPI_CHAR, MPI_INT, MPI_CHAR};

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

    AccPair local_boroughWeekAcc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

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

            string date = row[DATE];
            string borough = row[BOROUGH];
            Row newRow(row.getNumPersonsKilled());
            strncpy(newRow.date, date.c_str(), MAX_DATE_LENGTH);

            if (!borough.empty())
            {
                strncpy(newRow.borough, borough.c_str(), MAX_BOROUGH_LENGTH);

                // Populating dictionary for QUERY3
                if (brghDictionary.find(borough) == brghDictionary.end())
                    brghDictionary.insert({borough, indexBrgh++});
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

    // Initialization for scattering, evenly dividing dataset
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
    {
        num_brgh = brghDictionary.size();
    }
    MPI_Bcast(&num_brgh, 1, MPI_INT, 0, MPI_COMM_WORLD);

    brghKeys = (char(*)[MAX_BOROUGH_LENGTH])malloc(num_brgh * sizeof(*brghKeys));
    brghValues = new int[num_brgh];
    if (myrank == 0)
    {
        int i = 0;
        for (auto b : brghDictionary)
        {
            strncpy(brghKeys[i], b.first.c_str(), MAX_BOROUGH_LENGTH);
            brghValues[i] = b.second;
            i++;
        }
    }

    MPI_Bcast(brghKeys, num_brgh * MAX_BOROUGH_LENGTH, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast(brghValues, num_brgh, MPI_INT, 0, MPI_COMM_WORLD);

    if (myrank != 0)
    {
        for (int i = 0; i < num_brgh; i++)
            brghDictionary.insert({brghKeys[i], brghValues[i]});
    }

    // Scattering data to all workers
    localRows.resize(my_num_rows);
    MPI_Scatterv(dataScatter.data(), scatterCount, dataDispl, rowType, localRows.data(), my_num_rows, rowType, 0, MPI_COMM_WORLD);

    scatterDuration = MPI_Wtime() - scatterBegin;
    MPI_Gather(&scatterDuration, 1, MPI_DOUBLE, scatterTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // [2] Data processing
    procBegin = MPI_Wtime();

    cout << "[Proc. " + to_string(myrank) + "] Started processing dataset..." << endl;
    int brghIndex;

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
    for (int i = 0; i < my_num_rows; i++)
    {
        int lethal = (localRows[i].num_pers_killed > 0) ? 1 : 0;
        string borough = string(localRows[i].borough);
        int week, year, month = 0;

        if (!borough.empty())
        {
            week = getWeek(localRows[i].date);
            year = getYear(localRows[i].date);
            month = getMonth(localRows[i].date);

            // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
            // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
            if (week == 1 && month == 12)
                year++;
            else if ((week == 52 || week == 53) && month == 1)
                year--;

            year = year - 2012;
            week = week - 1;
        }

        /* Query3 */
        if (!borough.empty()) // if borough is not specified we're not interested
        {
            brghIndex = brghDictionary.at(borough);
            local_boroughWeekAcc[brghIndex][year][week].numAccidents++;
            local_boroughWeekAcc[brghIndex][year][week].numLethalAccidents += lethal;
        }
    }

    MPI_Reduce(local_boroughWeekAcc, global_boroughWeekAc, NUM_BOROUGH * NUM_YEARS * NUM_WEEKS_PER_YEAR, MPI_2INT, accPairSum, 0, MPI_COMM_WORLD);

    procDuration = MPI_Wtime() - procBegin;
    MPI_Gather(&procDuration, 1, MPI_DOUBLE, procTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // [3] Output results
    writeBegin = MPI_Wtime();

    if (myrank == 0)
    {
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
                    if (global_boroughWeekAc[b.second][i][j].numAccidents == 0)
                        continue;
                    numWeeks++;
                    numAccidents += global_boroughWeekAc[b.second][i][j].numAccidents;
                    numLethalAccidents += global_boroughWeekAc[b.second][i][j].numLethalAccidents;

                    cout << "(" << (i + 2012) << ")Week " << (j + 1);                                               // print (Year)Week N
                    cout << "\t\t\t num. accidents: " << global_boroughWeekAc[b.second][i][j].numAccidents << endl; // print numAccidents
                }
            }
            double avg = numLethalAccidents / numWeeks;
            cout << "[" << b.first << "] Avg. lethal accidents per week is: " << setprecision(2) << fixed << avg * 100 << "%";
            cout << "\t\t\tNum. accidents: " << numAccidents << "\t\tNum. lethal accidents: " << setprecision(0) << fixed << numLethalAccidents << endl;
            cout << endl;
        }
        cout << "Total boroughs parsed: " << brghDictionary.size() << "\n\n\n";
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

    delete[] brghKeys;
    delete[] brghValues;

    if (myrank == 0)
    {
        delete[] overallTimes;
        delete[] loadTimes;
        delete[] procTimes;
        delete[] writeTimes;
    }

    MPI_Finalize();
}
