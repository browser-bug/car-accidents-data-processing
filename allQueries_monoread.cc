#include <mpi.h>
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

#include "utils/csv_row/include/CSVIterator.h"

// Dataset
#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

#define DATE_LENGTH 11
// Query1
#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

// Query2
#define NUM_CONTRIBUTING_FACTORS 47
#define MAX_CF_PER_ROW 5
#define MAX_CF_LENGTH 55

// Query3
#define NUM_BOROUGH 5
#define MAX_BOROUGH_LENGTH 15

// Data structure representing a row used for pre-processing
typedef struct Row
{
    Row() : num_pers_killed(0), num_contributing_factors(0){};
    Row(int npk,
        int ncf)
        : num_pers_killed(npk), num_contributing_factors(ncf){};

    char date[DATE_LENGTH] = {};

    int num_pers_killed;

    char contributing_factors[MAX_CF_PER_ROW][MAX_CF_LENGTH] = {};
    int num_contributing_factors;

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

/* User-defind operation to manage the summation of AccPair during OpenMP reduction */
AccPair &operator+=(AccPair &out, const AccPair &in)
{
    out.numAccidents += in.numAccidents;
    out.numLethalAccidents += in.numLethalAccidents;
    return out;
}

/* User-defind operation to manage the summation of AccPair during MPI reduction */
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
    if (argc != 3)
    {
        cout << "Usage: " << argv[0] << " <num_omp_threads> <dataset_dimension>" << endl;
        exit(-1);
    }
    int num_omp_threads = stoi(argv[1]);
    string dataset_dim = argv[2];

    // int err;              // used for MPI error messages

    // Load dataset variables
    const string dataset_path = "dataset/";
    const string csv_path = dataset_path + "collisions_" + dataset_dim + ".csv";
    int csv_size = 0;

    // Support dictonaries
    int indexCF = 0;
    map<string, int> cfDictionary;
    char(*cfKeys)[MAX_CF_LENGTH]; // this contains all cf keys in the dataset
    int *cfValues;                // this contains all cf values in the dataset
    int num_cf;

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

    int global_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};                 // Global data structure for QUERY1
    AccPair global_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};                      // Global data structure for QUERY2
    AccPair global_boroughWeekAc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {}; // Global data structure for QUERY3

    // MPI Datatypes definitions
    MPI_Datatype rowType;
    int rowLength[] = {
        DATE_LENGTH,
        1,
        MAX_CF_PER_ROW * MAX_CF_LENGTH,
        1,
        MAX_BOROUGH_LENGTH};
    MPI_Aint rowDisplacements[] = {
        offsetof(Row, date),
        offsetof(Row, num_pers_killed),
        offsetof(Row, contributing_factors),
        offsetof(Row, num_contributing_factors),
        offsetof(Row, borough)};
    MPI_Datatype rowTypes[] = {MPI_CHAR, MPI_INT, MPI_CHAR, MPI_INT, MPI_CHAR};

    // FIX this is no longer needed as MPI provides the MPI_2INT datatype
    // MPI_Datatype accPairType;

    // MPI Operators definitions
    MPI_Op accPairSum;

    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);

    MPI_Type_create_struct(5, rowLength, rowDisplacements, rowTypes, &rowType);
    MPI_Type_commit(&rowType);

    MPI_Op_create(pairSum, true, &accPairSum);
#pragma omp declare reduction(accPairSum:AccPair \
                              : omp_out += omp_in)

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

    int local_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};                  // Local data structure for QUERY1
    AccPair local_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};                       // Local data structure for QUERY2
    AccPair local_boroughWeekAcc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {}; // Local data structure for QUERY3

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
            vector<string> cfs = row.getContributingFactors();

            Row newRow(row.getNumPersonsKilled(), 0);

            strncpy(newRow.date, date.c_str(), DATE_LENGTH);

            for (unsigned int k = 0; k < cfs.size(); k++)
            {
                strncpy(newRow.contributing_factors[k], cfs[k].c_str(), MAX_CF_LENGTH);
                newRow.num_contributing_factors++;

                // Populating dictionary for QUERY2
                if (cfDictionary.find(cfs[k]) == cfDictionary.end())
                    cfDictionary.insert({cfs[k], indexCF++});
            }

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

    MPI_Bcast(&csv_size, 1, MPI_INT, 0, MPI_COMM_WORLD); // FIX this isn't needed
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
        num_cf = cfDictionary.size();
        num_brgh = brghDictionary.size();
    }
    MPI_Bcast(&num_cf, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&num_brgh, 1, MPI_INT, 0, MPI_COMM_WORLD);

    cfKeys = (char(*)[MAX_CF_LENGTH])malloc(num_cf * sizeof(*cfKeys));
    cfValues = new int[num_cf];
    brghKeys = (char(*)[MAX_BOROUGH_LENGTH])malloc(num_brgh * sizeof(*brghKeys));
    brghValues = new int[num_brgh];
    if (myrank == 0)
    {
        int i = 0;
        for (auto cf : cfDictionary)
        {
            strncpy(cfKeys[i], cf.first.c_str(), MAX_CF_LENGTH);
            cfValues[i] = cf.second;
            i++;
        }
        i = 0;
        for (auto b : brghDictionary)
        {
            strncpy(brghKeys[i], b.first.c_str(), MAX_BOROUGH_LENGTH);
            brghValues[i] = b.second;
            i++;
        }
    }

    MPI_Bcast(cfKeys, num_cf * MAX_CF_LENGTH, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast(cfValues, num_cf, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(brghKeys, num_brgh * MAX_BOROUGH_LENGTH, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast(brghValues, num_brgh, MPI_INT, 0, MPI_COMM_WORLD);

    if (myrank != 0)
    {
        for (int i = 0; i < num_cf; i++)
            cfDictionary.insert({cfKeys[i], cfValues[i]});
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
    omp_set_num_threads(num_omp_threads);
    int cfIndex, brghIndex;

    int dynChunk = (int)round(my_num_rows * 0.02); // this tunes the chunk size exploited by dynamic scheduling based on percentage

// Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
#pragma omp parallel for default(shared) schedule(dynamic, dynChunk) private(cfIndex, brghIndex) reduction(+                                            \
                                                                                                           : local_lethAccPerWeek) reduction(accPairSum \
                                                                                                                                             : local_accAndPerc, local_boroughWeekAcc)
    for (int i = 0; i < my_num_rows; i++)
    {
        int lethal = (localRows[i].num_pers_killed > 0) ? 1 : 0;
        string borough = string(localRows[i].borough);
        int week, year, month = 0;

        if (lethal || !borough.empty())
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

        /* Query2 */
        for (int k = 0; k < localRows[i].num_contributing_factors; k++)
        {
            cfIndex = cfDictionary.at(string(localRows[i].contributing_factors[k]));
            (local_accAndPerc[cfIndex].numAccidents)++;
            (local_accAndPerc[cfIndex].numLethalAccidents) += lethal;
        }

        /* Query3 */
        if (!borough.empty()) // if borough is not specified we're not interested
        {
            brghIndex = brghDictionary.at(borough);
            local_boroughWeekAcc[brghIndex][year][week].numAccidents++;
            local_boroughWeekAcc[brghIndex][year][week].numLethalAccidents += lethal;
        }
    }

    // Query1
    MPI_Reduce(local_lethAccPerWeek, global_lethAccPerWeek, (NUM_YEARS * NUM_WEEKS_PER_YEAR), MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    // Query2
    MPI_Reduce(local_accAndPerc, global_accAndPerc, NUM_CONTRIBUTING_FACTORS, MPI_2INT, accPairSum, 0, MPI_COMM_WORLD);
    // Query3
    MPI_Reduce(local_boroughWeekAcc, global_boroughWeekAc, NUM_BOROUGH * NUM_YEARS * NUM_WEEKS_PER_YEAR, MPI_2INT, accPairSum, 0, MPI_COMM_WORLD);

    procDuration = MPI_Wtime() - procBegin;
    MPI_Gather(&procDuration, 1, MPI_DOUBLE, procTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // [3] Output results
    writeBegin = MPI_Wtime();

    if (myrank == 0)
    {
        // Open output file
        ofstream outFile("results/result_monoread_" + dataset_dim + ".txt");
        outFile.clear();

        // Print Query1 results
        outFile << "********* QUERY 1 *********" << endl;
        int totalWeeks = 0;
        int totalAccidents = 0;

        for (int year = 0; year < NUM_YEARS; year++)
        {
            for (int week = 0; week < NUM_WEEKS_PER_YEAR; week++)
            {
                int numLethAcc = global_lethAccPerWeek[year][week];
                if (numLethAcc > 0)
                {
                    outFile << "(" << (year + 2012) << ")Week: " << (week + 1) << "\t\t\t Num. lethal accidents: ";
                    outFile << numLethAcc << endl;
                    totalAccidents += numLethAcc;
                    totalWeeks++;
                }
            }
        }
        outFile << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << "\n\n\n";

        // Print Query2 results
        outFile << "********* QUERY 2 *********" << endl;
        for (auto el : cfDictionary)
        {
            double perc = double(global_accAndPerc[el.second].numLethalAccidents) / global_accAndPerc[el.second].numAccidents;
            outFile << el.first << endl
                    << "\t\tNum. of accidents: " << global_accAndPerc[el.second].numAccidents
                    << "\t\t\t\t\tPerc. lethal accidents: " << setprecision(4) << fixed << perc * 100 << "%"
                    << endl;
        }
        outFile << "\nTotal contributing factors parsed: " << cfDictionary.size() << "\n\n\n";

        // Print Query3 results
        outFile << "********* QUERY 3 *********" << endl;
        for (auto b : brghDictionary)
        {
            int numWeeks = 0;
            int numAccidents = 0;
            double numLethalAccidents = 0;

            outFile << "Borough: " << b.first << endl;
            for (int i = 0; i < NUM_YEARS; i++) // for each year
            {
                for (int j = 0; j < NUM_WEEKS_PER_YEAR; j++) // for each week
                {
                    if (global_boroughWeekAc[b.second][i][j].numAccidents == 0)
                        continue;
                    numWeeks++;
                    numAccidents += global_boroughWeekAc[b.second][i][j].numAccidents;
                    numLethalAccidents += global_boroughWeekAc[b.second][i][j].numLethalAccidents;

                    outFile << "(" << (i + 2012) << ")Week " << (j + 1);                                               // print (Year)Week N
                    outFile << "\t\t\t num. accidents: " << global_boroughWeekAc[b.second][i][j].numAccidents << endl; // print numAccidents
                }
            }
            double avg = numLethalAccidents / numWeeks;
            outFile << "[" << b.first << "] Avg. lethal accidents per week is: " << setprecision(2) << fixed << avg * 100 << "%";
            outFile << "\t\t\tNum. accidents: " << numAccidents << "\t\tNum. lethal accidents: " << setprecision(0) << fixed << numLethalAccidents << endl;
            outFile << endl;
        }
        outFile << "Total boroughs parsed: " << brghDictionary.size() << "\n\n\n";
        outFile.close();
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
        cout << "NUM. OMP THREADS:\t" << num_omp_threads << endl;

        double avgOverall, avgLoading, avgScattering, avgProcessing, avgWriting = 0;
        for (int i = 0; i < num_workers; i++)
        {
            cout << "Process {" << i << "}\t";
            if (i == 0)
                cout << "Loading(" << loadTimes[0] << "), ";
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

        // Open result file
        string statsFileName = "stats_monoread_" + dataset_dim + "_" + to_string(num_workers) + "p_" + to_string(num_omp_threads) + "t.csv";
        ifstream checkFile("stats/" + statsFileName);
        if (!checkFile.good())
        {
            // if csv doesn't exists create file and add header first
            ofstream outFile("stats/" + statsFileName);
            outFile << "Loading,Scattering,Processing,Writing,Overall" << endl;
            outFile.close();
        }
        ofstream outFile("stats/" + statsFileName, ios::app);
        outFile << loadTimes[0] << ","
                << (num_workers > 1 ? avgScattering / (num_workers - 1) : 0) << ","
                << (avgProcessing / num_workers) << ","
                << writeTimes[0] << ","
                << (avgOverall / num_workers)
                << endl;
        outFile.close();

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
