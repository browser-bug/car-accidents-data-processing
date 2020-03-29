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

#include "../utilities/CSVIterator.h"

#define DEBUG 0

// Dataset
#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

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
    Row() : week(0), month(0), year(0), num_pers_killed(0), num_contributing_factors(0){};
    Row(int w,
        int m,
        int y,
        int npk,
        int ncf)
        : week(w), month(m), year(y), num_pers_killed(npk), num_contributing_factors(ncf){};

    int week;
    int month;
    int year;

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
    bool testing = true; // switch between dataset for testing and original dataset
    // int err;             // used for MPI error messages

    string csv_path = testing ? "../dataset/data_test.csv" : "../dataset/NYPD_Motor_Vehicle_Collisions.csv";
    ifstream file(csv_path);

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

    // LOADING THE DATASET
    int csv_size = testing ? TEST_SIZE : ORIGINAL_SIZE;
    // csv_size = 29996; // Set the first N rows to be read
    int myrank, num_workers;

    // years [2012, 2013, 2014, 2015, 2016, 2017]
    // Global data structure for QUERY3
    AccPair global_boroughWeekAc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    // MPI Datatypes definitions
    MPI_Datatype rowType;
    int rowLength[] = {
        1,
        1,
        1,
        1,
        MAX_CF_PER_ROW * MAX_CF_LENGTH,
        1,
        MAX_BOROUGH_LENGTH};
    MPI_Aint rowDisplacements[] = {
        offsetof(Row, week),
        offsetof(Row, month),
        offsetof(Row, year),
        offsetof(Row, num_pers_killed),
        offsetof(Row, contributing_factors),
        offsetof(Row, num_contributing_factors),
        offsetof(Row, borough)};
    MPI_Datatype rowTypes[] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_CHAR, MPI_INT, MPI_CHAR};

    MPI_Datatype accPairType;

    // MPI Operators definitions
    MPI_Op accPairSum;

    MPI_Init(&argc, &argv);

    if (DEBUG)
    {
        volatile int ifl = 0;
        char hostname[256];
        gethostname(hostname, sizeof(hostname));
        printf("PID %d on %s ready for attach\n", getpid(), hostname);
        fflush(stdout);
        while (0 == ifl)
            sleep(5);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);

    MPI_Type_create_struct(7, rowLength, rowDisplacements, rowTypes, &rowType);
    MPI_Type_commit(&rowType);
    MPI_Type_contiguous(2, MPI_INT, &accPairType);
    MPI_Type_commit(&accPairType);

    MPI_Op_create(pairSum, true, &accPairSum);

    // Local data structures
    vector<Row> dataScatter;
    vector<Row> localRows;
    int scatterCount[num_workers];
    int dataDispl[num_workers];
    int my_num_rows;
    int my_row_displ;

    // Local data structure for QUERY3
    AccPair local_boroughWeekAcc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

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

    if (myrank == 0)
    {
        CSVRow row;

        for (int i = 0; i < csv_size; i++)
        {
            if (i == 0)
                file >> row >> row; // skip the header
            else
                file >> row;

            string date = row[DATE];
            string borough = row[BOROUGH];
            vector<string> cfs = row.getContributingFactors();

            dataScatter.push_back(
                Row(getWeek(date), getMonth(date), getYear(date), row.getNumPersonsKilled(), 0));

            if (!borough.empty())
            {
                strncpy(dataScatter[i].borough, borough.c_str(), MAX_BOROUGH_LENGTH);

                // Populating dictionary for QUERY3
                if (brghDictionary.find(borough) == brghDictionary.end())
                    brghDictionary.insert({borough, indexBrgh++});
            }

            for (unsigned int k = 0; k < cfs.size(); k++)
            {
                {
                    strncpy(dataScatter[i].contributing_factors[k], cfs[k].c_str(), MAX_CF_LENGTH);
                    dataScatter[i].num_contributing_factors++;
                }

                // Populating dictionary for QUERY2
                if (cfDictionary.find(cfs[k]) == cfDictionary.end())
                    cfDictionary.insert({cfs[k], indexCF++});
            }
        }
    }

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

/////////////
/* QUERY 3 */
/////////////

// Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
#pragma omp parallel for shared(local_boroughWeekAcc) num_threads(8)
    for (int i = 0; i < my_num_rows; i++)
    {
        string borough = string(localRows[i].borough);
        if (borough.empty()) // if borough is not specified we're not interested
            continue;

        int lethal = (localRows[i].num_pers_killed > 0) ? 1 : 0;

        int year; // used for indexing final data structure
        int week; // used for indexing final data structure

        // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
        // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
        if (localRows[i].week == 1 && localRows[i].month == 12)
            localRows[i].year++;
        else if ((localRows[i].week == 52 || localRows[i].week == 53) && localRows[i].month == 1)
            localRows[i].year--;

        year = localRows[i].year - 2012;
        week = localRows[i].week - 1;

        int index = brghDictionary.at(borough);
#pragma omp atomic
        local_boroughWeekAcc[index][year][week].numAccidents++;
#pragma omp atomic
        local_boroughWeekAcc[index][year][week].numLethalAccidents += lethal;
    }

    MPI_Reduce(local_boroughWeekAcc, global_boroughWeekAc, NUM_BOROUGH * NUM_YEARS * NUM_WEEKS_PER_YEAR, accPairType, accPairSum, 0, MPI_COMM_WORLD);

    if (myrank == 0)
    {
        // Print results
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
        cout << "Total boroughs parsed: " << brghDictionary.size();
    }

    delete[] cfKeys;
    delete[] cfValues;
    delete[] brghKeys;
    delete[] brghValues;

    MPI_Finalize();
}
