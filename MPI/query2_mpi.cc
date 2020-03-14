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

#include "../utilities/CSVIterator.h"

#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

#define NUM_CONTRIBUTING_FACTORS 47

#define MAX_CF_PER_ROW 5
#define MAX_CF_LENGTH 55

#define DEBUG 0

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
} Row;

typedef struct AccPair
{
    AccPair() : numAccidents(0), numPersKilled(0){};
    AccPair(int na,
            int nla) : numAccidents(na), numPersKilled(nla){};

    int numAccidents;
    int numPersKilled;
} AccPair;

/* We implement a user-defind operation to manage the summation of AccPair */
void pairSum(void *inputBuffer, void *outputBuffer, int *len, MPI_Datatype *dptr)
{
    AccPair *in = (AccPair *)inputBuffer;
    AccPair *inout = (AccPair *)outputBuffer;

    for (int i = 0; i < *len; ++i)
    {
        inout[i].numAccidents += in[i].numAccidents;
        inout[i].numPersKilled += in[i].numPersKilled;
    }
}

using namespace std;

int main(int argc, char **argv)
{

    bool testing = false; // switch between dataset for testing and original dataset
    int err;              // used for MPI error messages

    string csv_path = testing ? "../dataset/data_test.csv" : "../dataset/NYPD_Motor_Vehicle_Collisions.csv";
    ifstream file(csv_path);

    // Support dictonaries
    int indexCF = 0;
    map<string, int> cfDictionary;
    char(*cfKeys)[MAX_CF_LENGTH]; // this contains all cf keys in the dataset
    int *cfValues;                // this contains all cf values in the dataset
    int num_cf;

    int indexB = 0;
    map<string, int> boroughs;

    // LOADING THE DATASET
    int csv_size = testing ? TEST_SIZE : ORIGINAL_SIZE;
    // csv_size = 4; // Set the first N rows to be read
    int myrank, num_workers;

    // years [2012, 2013, 2014, 2015, 2016, 2017]
    // Global data structure for QUERY1
    int global_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};
    // Global data structure for QUERY2
    AccPair global_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};

    // MPI Datatypes definitions
    MPI_Datatype rowType;
    int rowLength[] = {
        1,
        1,
        1,
        1,
        MAX_CF_PER_ROW * MAX_CF_LENGTH,
        1};
    MPI_Aint rowDisplacements[] = {
        offsetof(Row, week),
        offsetof(Row, month),
        offsetof(Row, year),
        offsetof(Row, num_pers_killed),
        offsetof(Row, contributing_factors),
        offsetof(Row, num_contributing_factors)};
    MPI_Datatype rowTypes[] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_CHAR, MPI_INT};

    MPI_Datatype accPairType;
    // TODO add these two lines if everything works out
    // MPI_Type_contiguous( 2, MPI_INT, &accPairType );
    // MPI_Type_commit( &accPairType );
    int aptLength[] = {1, 1};
    MPI_Aint aptDisplacements[] = {
        offsetof(AccPair, numAccidents),
        offsetof(AccPair, numPersKilled)};
    MPI_Datatype aptTypes[] = {MPI_INT, MPI_INT};

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

    MPI_Type_create_struct(6, rowLength, rowDisplacements, rowTypes, &rowType);
    MPI_Type_commit(&rowType);
    MPI_Type_create_struct(2, aptLength, aptDisplacements, aptTypes, &accPairType);
    MPI_Type_commit(&accPairType);

    MPI_Op_create(pairSum, true, &accPairSum);

    vector<Row> dataScatter;
    vector<Row> localRows;
    int scatterCount[num_workers];
    int dataDispl[num_workers];
    int my_num_rows;
    int my_row_displ;

    // Local data structure for QUERY1
    int local_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};
    // Local data structure for QUERY2
    AccPair local_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};

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
        MPI_Recv(&my_num_rows, 1, MPI_INT, 0, 13, MPI_COMM_WORLD, NULL);
        MPI_Recv(&my_row_displ, 1, MPI_INT, 0, 14, MPI_COMM_WORLD, NULL);
    }

    localRows.resize(my_num_rows);

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
            dataScatter.push_back(
                Row(getWeek(date), getMonth(date), getYear(date), row.getNumPersonsKilled(), 0));
            vector<string> cfs = row.getContributingFactors();
            for (int k = 0; k < cfs.size(); k++)
            {
                strcpy(dataScatter[i].contributing_factors[k], cfs[k].c_str());
                dataScatter[i].num_contributing_factors++;

                // Populating dictonary for QUERY2
                if (cfDictionary.find(cfs[k]) == cfDictionary.end())
                    cfDictionary.insert({cfs[k], indexCF++});
            }

            // cout << "(" << dataScatter[i].year
            //      << ") Week " << dataScatter[i].week
            //      << " num.pers.killed " << dataScatter[i].num_pers_killed
            //      << " first contributing_factor [" << dataScatter[i].contributing_factors[0] << "]"
            //      << " num cf " << dataScatter[i].num_contributing_factors
            //      << endl;

            // Populating dictonary for QUERY3
            string b = row[BOROUGH];
            if (!b.empty() && b.compare("Unspecified") && boroughs.find(b) == boroughs.end())
                boroughs.insert({b, indexB++});
        }
    }

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

    err = MPI_Scatterv(dataScatter.data(), scatterCount, dataDispl, rowType, localRows.data(), my_num_rows, rowType, 0, MPI_COMM_WORLD);
    if (err != MPI_SUCCESS)
    {
        cout << "Scattering failed with error code " << err;
        exit(0);
    }

    /////////////
    /* QUERY 2 */
    /////////////

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
    for (int i = 0; i < my_num_rows; i++)
    {
        // cout << "WorkerID: " << myrank << " num CF. " << localRows[i].num_contributing_factors << endl;
        if (localRows[i].num_contributing_factors == 0)
            continue;

        // int lethal = (localRows[i].num_pers_killed > 0) ? 1 : 0;
        for (int k = 0; k < localRows[i].num_contributing_factors; k++)
        {
            string cf(localRows[i].contributing_factors[k]);
            (local_accAndPerc[cfDictionary.at(cf)].numAccidents)++;
            (local_accAndPerc[cfDictionary.at(cf)].numPersKilled) += localRows[i].num_pers_killed;
        }
    }

    err = MPI_Reduce(local_accAndPerc, global_accAndPerc, NUM_CONTRIBUTING_FACTORS, accPairType, accPairSum, 0, MPI_COMM_WORLD);
    if (err != MPI_SUCCESS)
    {
        cout << "Reduce failed with error code " << err;
        exit(0);
    }

    if (myrank == 0)
    {
        // Print results
        for (auto el : cfDictionary)
        {
            double perc = double(global_accAndPerc[el.second].numPersKilled) / global_accAndPerc[el.second].numAccidents;
            cout << el.first << endl
                 << "\t\tNum. of accidents: " << global_accAndPerc[el.second].numAccidents
                 << "\t\t\tPerc. lethal accidents: " << setprecision(2) << fixed << perc * 100 << "%"
                 << endl;
        }
        cout << "Total CF parsed: " << cfDictionary.size();
    }

    MPI_Finalize();
}