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

#include "../utilities/csv_row/include/CSVIterator.h"

#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

#define DATE_LENGTH 11

#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

#define NUM_CONTRIBUTING_FACTORS 47

// Data structure representing a row used for pre-processing
typedef struct Row
{
    Row() : num_pers_killed(0){};
    Row(int npk)
        : num_pers_killed(npk){};

    char date[DATE_LENGTH] = {};

    int num_pers_killed;
} Row;

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

    // MPI variables
    int myrank, num_workers;
    double *overallTimes; // these contain time stats for each process (e.g. overallTimes[3] -> overall duration time of rank = 3 process)
    double *loadTimes;
    double *scatterTimes;
    double *procTimes;
    double *writeTimes;

    int global_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    // MPI Datatype creation
    MPI_Datatype rowType;
    int rowLength[] = {DATE_LENGTH, 1};
    MPI_Aint rowDisplacements[] = {
        offsetof(Row, date),
        offsetof(Row, num_pers_killed)};
    MPI_Datatype rowTypes[] = {MPI_CHAR, MPI_INT};

    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);

    MPI_Type_create_struct(2, rowLength, rowDisplacements, rowTypes, &rowType);
    MPI_Type_commit(&rowType);

    // Local data structure for QUERY1
    vector<Row> dataScatter;
    vector<Row> localRows;
    int scatterCount[num_workers];
    int dataDispl[num_workers];
    int my_num_rows;
    int my_row_displ;

    int local_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    // Timing stats variables
    double overallBegin, overallDuration; // overall application duration time
    double loadBegin, loadDuration;       // loading phase duration time
    double scatterBegin, scatterDuration; // scattering phase duration time
    double procBegin, procDuration;       // processing phase duration time
    double writeBegin, writeDuration;     // printing stats duration time

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
            Row newRow(row.getNumPersonsKilled());
            strncpy(newRow.date, date.c_str(), DATE_LENGTH);
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

    // Scattering data to all workers
    localRows.resize(my_num_rows);
    MPI_Scatterv(dataScatter.data(), scatterCount, dataDispl, rowType, localRows.data(), my_num_rows, rowType, 0, MPI_COMM_WORLD);

    scatterDuration = MPI_Wtime() - scatterBegin;
    MPI_Gather(&scatterDuration, 1, MPI_DOUBLE, scatterTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // [2] Data processing
    procBegin = MPI_Wtime();

    cout << "[Proc. " + to_string(myrank) + "] Started processing dataset..." << endl;

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
    for (int i = 0; i < my_num_rows; i++)
    {
        int lethal = (localRows[i].num_pers_killed > 0) ? 1 : 0;
        int week, year, month = 0;

        if (lethal)
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

        /* Query1 */
        if (lethal)
            local_lethAccPerWeek[year][week]++;
    }

    MPI_Reduce(local_lethAccPerWeek, global_lethAccPerWeek, (NUM_YEARS * NUM_WEEKS_PER_YEAR), MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    procDuration = MPI_Wtime() - procBegin;
    MPI_Gather(&procDuration, 1, MPI_DOUBLE, procTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // [3] Output results
    writeBegin = MPI_Wtime();

    if (myrank == 0)
    {
        // Print Query1 results
        cout << "********* QUERY 1 *********" << endl;
        int totalWeeks = 0;
        int totalAccidents = 0;

        for (int year = 0; year < NUM_YEARS; year++)
        {
            for (int week = 0; week < NUM_WEEKS_PER_YEAR; week++)
            {
                int numLethAcc = global_lethAccPerWeek[year][week];
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

    if (myrank == 0)
    {
        delete[] overallTimes;
        delete[] loadTimes;
        delete[] procTimes;
        delete[] writeTimes;
    }

    MPI_Finalize();
}
