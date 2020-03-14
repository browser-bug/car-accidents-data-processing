#include <mpi.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <string>

#include "../utilities/CSVIterator.h"

#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

#define NUM_CONTRIBUTING_FACTORS 47

// Data structure representing a row used for pre-processing
typedef struct Row
{
    Row() : week(0), month(0), year(0), num_pers_killed(0){};
    Row(int w,
        int m,
        int y,
        int npk) : week(w), month(m), year(y), num_pers_killed(npk){};

    int week;
    int month;
    int year;

    int num_pers_killed;
} Row;

using namespace std;

int main(int argc, char **argv)
{
    bool testing = false; // switch between dataset for testing and original dataset
    int err;              // used for MPI error messages

    string csv_path = testing ? "../dataset/data_test.csv" : "../dataset/NYPD_Motor_Vehicle_Collisions.csv";
    ifstream file(csv_path);

    // LOADING THE DATASET
    int csv_size = testing ? TEST_SIZE : ORIGINAL_SIZE;
    int myrank, num_workers;

    // years [2012, 2013, 2014, 2015, 2016, 2017]
    int lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    // MPI Datatype creation
    MPI_Datatype rowType;
    int rowLength[4] = {1, 1, 1, 1};
    MPI_Aint rowDisplacements[4] = {offsetof(Row, week), offsetof(Row, month), offsetof(Row, year), offsetof(Row, num_pers_killed)};
    MPI_Datatype rowTypes[4] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT};

    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);

    vector<Row> dataScatter;
    vector<Row> localRows;
    int scatterCount[num_workers];
    int dataDispl[num_workers];
    int my_num_rows;
    int my_row_displ;

    // Local data structure for QUERY1
    int local_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    MPI_Type_create_struct(4, rowLength, rowDisplacements, rowTypes, &rowType);
    MPI_Type_commit(&rowType);

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
                Row(getWeek(date), getMonth(date), getYear(date), row.getNumPersonsKilled()));
            // cout << "(" << dataScatter[i].year << ") Week " << dataScatter[i].week << " num.pers.killed " << dataScatter[i].num_pers_killed << endl;
        }
    }

    err = MPI_Scatterv(dataScatter.data(), scatterCount, dataDispl, rowType, localRows.data(), my_num_rows, rowType, 0, MPI_COMM_WORLD);
    if (err != MPI_SUCCESS)
    {
        cout << "Scattering failed with error code " << err;
        exit(0);
    }

    /////////////
    /* QUERY 1 */
    /////////////

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
    for (int i = 0; i < my_num_rows; i++)
    {
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

        if (localRows[i].num_pers_killed > 0)
            local_lethAccPerWeek[year][week]++;
        // cout << "WorkerID: " << myrank << "\t\tnum_pers_killed: " << local_lethAccPerWeek[year][week] << endl;
    }

    MPI_Reduce(local_lethAccPerWeek, lethAccPerWeek, (NUM_YEARS * NUM_WEEKS_PER_YEAR), MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    if (myrank == 0)
    {
        // Print results
        int totalWeeks = 0;
        int totalAccidents = 0;

        for (int year = 0; year < NUM_YEARS; year++)
        {
            for (int week = 0; week < NUM_WEEKS_PER_YEAR; week++)
            {
                int numLethAcc = lethAccPerWeek[year][week];
                if (numLethAcc > 0)
                {
                    cout << "(" << (year + 2012) << ")Week: " << (week + 1) << "\t\t\t Num. lethal accidents: ";
                    cout << numLethAcc << endl;
                    totalAccidents += numLethAcc;
                    totalWeeks++;
                }
            }
        }
        cout << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << endl;
    }

    MPI_Finalize();
}