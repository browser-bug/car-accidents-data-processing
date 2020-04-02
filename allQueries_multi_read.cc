#include <mpi.h>
#include <omp.h>
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

#include "utilities/CSVIterator.h"

#define DEBUG 0

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
    // if (argc != 2)
    // {
    //     cout << "Usage: " << argv[0] << " <num_omp_threads>" << endl;
    //     exit(-1);
    // }
    // TODO this will be assigned by user input
    int num_omp_threads = 4;
    string dataset_dim = "1M";
    // num_omp_threads = stoi(argv[1]);

    bool testing = false; // switch between dataset for testing and original dataset

    // Load dataset variables
    const string dataset_path = "dataset/";
    const string csv_path = testing ? dataset_path + "data_test.csv" : dataset_path + "collisions_" + dataset_dim + ".csv";
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

    // Load dataset variables
    MPI_File input_file;
    const int overlap = 200; // used to define overlapping intervals during csv reading phase

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

    // FIX this is no longer needed as MPI provides the MPI_2INT datatype
    // MPI_Datatype accPairType;

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

    MPI_Op_create(pairSum, true, &accPairSum);

    // Local data structures
    vector<Row> localRows;
    int my_num_rows;

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

    // TODO add stackoverflow credits https://stackoverflow.com/questions/12939279/mpi-reading-from-a-text-file
    MPI_File_open(MPI_COMM_WORLD, csv_path.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &input_file);

    MPI_Offset globalstart;
    int mysize;
    char *chunk;

    /* read in relevant chunk of file into "chunk",
     * which starts at location in the file globalstart
     * and has size mysize 
     */
    {
        MPI_Offset globalend;
        MPI_Offset filesize;

        /* figure out who reads what */
        MPI_File_get_size(input_file, &filesize);
        filesize--; /* get rid of text file eof */
        mysize = filesize / num_workers;

        globalstart = myrank * mysize;

        /* last proc gets the biggest slice in case mysize division has a reminder */
        if (myrank == num_workers - 1)
            globalend = filesize;

        /* add overlap to the end of everyone's chunk except last proc... */
        if (myrank != num_workers - 1)
            globalend = (globalstart + mysize - 1) + overlap;

        mysize = globalend - globalstart + 1; // fix the size of every proc

        /* allocate memory */
        chunk = new char[mysize + 1];

        /* everyone reads in their part */
        MPI_File_read_at_all(input_file, globalstart, chunk, mysize, MPI_CHAR, MPI_STATUS_IGNORE);
        chunk[mysize] = '\0';

        /*
        * everyone calculate what their start and end *really* are by going
        * from the first newline after start to the first newline after the
        * overlap region starts (eg, after end - overlap + 1)
        */

        int locstart = 0, locend = mysize - 1;
        if (myrank != 0) // except the first proc
        {
            while (chunk[locstart] != '\n')
                locstart++;
            locstart++;
        }
        if (myrank != num_workers - 1) // except the last proc
        {
            locend -= overlap;
            while (chunk[locend] != '\n')
                locend++;
        }
        mysize = locend - locstart + 1;

        CSVRow row;
        string line;

        cout << "[Proc. " + to_string(myrank) + "] Started loading dataset..." << endl;
        for (int i = locstart; i <= locend; i++)
        {
            if (chunk[i] != '\n')
            {
                line.push_back(chunk[i]);
            }
            else
            {
                row.readRowFromString(line);
                line.clear();
                if (!row[TIME].compare("TIME")) // TODO: find a nicer way to skip the header
                    continue;

                string date = row[DATE];
                string borough = row[BOROUGH];
                vector<string> cfs = row.getContributingFactors();

                Row newRow(getWeek(date), getMonth(date), getYear(date), row.getNumPersonsKilled(), 0);

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

                localRows.push_back(newRow);
            }
        }

        delete[] chunk;
        my_num_rows = localRows.size();
        MPI_File_close(&input_file);
    }

    loadDuration = MPI_Wtime() - loadBegin;
    MPI_Gather(&loadDuration, 1, MPI_DOUBLE, loadTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // Broadcasting the dictionaries to all processes
    scatterBegin = MPI_Wtime();

    int my_num_cf, my_num_brgh = 0;
    if (myrank != 0)
    {
        my_num_cf = cfDictionary.size(), my_num_brgh = brghDictionary.size();
        cfKeys = (char(*)[MAX_CF_LENGTH])malloc(my_num_cf * sizeof(*cfKeys));
        brghKeys = (char(*)[MAX_BOROUGH_LENGTH])malloc(my_num_brgh * sizeof(*brghKeys));

        int i = 0;
        for (auto cf : cfDictionary)
        {
            strncpy(cfKeys[i], cf.first.c_str(), MAX_CF_LENGTH);
            i++;
        }
        i = 0;
        for (auto b : brghDictionary)
        {
            strncpy(brghKeys[i], b.first.c_str(), MAX_BOROUGH_LENGTH);
            i++;
        }

        MPI_Send(&my_num_cf, 1, MPI_INT, 0, 12, MPI_COMM_WORLD);
        MPI_Send(cfKeys, my_num_cf * MAX_CF_LENGTH, MPI_CHAR, 0, 12, MPI_COMM_WORLD);
        MPI_Send(&my_num_brgh, 1, MPI_INT, 0, 13, MPI_COMM_WORLD);
        MPI_Send(brghKeys, my_num_brgh * MAX_BOROUGH_LENGTH, MPI_CHAR, 0, 13, MPI_COMM_WORLD);

        delete[] cfKeys;
        delete[] brghKeys;
    }

    if (myrank == 0)
    {
        for (int rank = 1; rank < num_workers; rank++)
        {
            MPI_Recv(&my_num_cf, 1, MPI_INT, rank, 12, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            cfKeys = (char(*)[MAX_CF_LENGTH])malloc(my_num_cf * sizeof(*cfKeys));
            MPI_Recv(cfKeys, my_num_cf * MAX_CF_LENGTH, MPI_CHAR, rank, 12, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int i = 0; i < my_num_cf; i++)
                cfDictionary.insert({cfKeys[i], indexCF++});

            MPI_Recv(&my_num_brgh, 1, MPI_INT, rank, 13, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            brghKeys = (char(*)[MAX_BOROUGH_LENGTH])malloc(my_num_brgh * sizeof(*brghKeys));
            MPI_Recv(brghKeys, my_num_brgh * MAX_BOROUGH_LENGTH, MPI_CHAR, rank, 13, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int i = 0; i < my_num_brgh; i++)
                brghDictionary.insert({brghKeys[i], indexBrgh++});

            delete[] cfKeys;
            delete[] brghKeys;
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
        cfDictionary.clear();
        brghDictionary.clear();

        for (int i = 0; i < num_cf; i++)
            cfDictionary.insert({cfKeys[i], cfValues[i]});
        for (int i = 0; i < num_brgh; i++)
            brghDictionary.insert({brghKeys[i], brghValues[i]});
    }

    scatterDuration += MPI_Wtime() - scatterBegin;
    MPI_Gather(&scatterDuration, 1, MPI_DOUBLE, scatterTimes, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // [2] Data processing
    procBegin = MPI_Wtime();

    cout << "[Proc. " + to_string(myrank) + "] Started processing dataset..." << endl;
    omp_set_num_threads(num_omp_threads);
    int year, week, lethal;
    int cfIndex, brghIndex;
    string borough;
// Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
#pragma omp parallel for default(shared) schedule(dynamic) private(year, week, lethal, cfIndex, brghIndex, borough)
    for (int i = 0; i < my_num_rows; i++)
    {
        lethal = (localRows[i].num_pers_killed > 0) ? 1 : 0;
        borough = string(localRows[i].borough);

        // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
        // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
        if (localRows[i].week == 1 && localRows[i].month == 12)
            localRows[i].year++;
        else if ((localRows[i].week == 52 || localRows[i].week == 53) && localRows[i].month == 1)
            localRows[i].year--;

        year = localRows[i].year - 2012;
        week = localRows[i].week - 1;

        /* Query1 */
        if (lethal)
#pragma omp atomic
            local_lethAccPerWeek[year][week]++;

        /* Query2 */
        for (int k = 0; k < localRows[i].num_contributing_factors; k++)
        {
            cfIndex = cfDictionary.at(string(localRows[i].contributing_factors[k]));
#pragma omp atomic
            (local_accAndPerc[cfIndex].numAccidents)++;
#pragma omp atomic
            (local_accAndPerc[cfIndex].numLethalAccidents) += lethal;
        }

        /* Query3 */
        if (!borough.empty()) // if borough is not specified we're not interested
        {
            brghIndex = brghDictionary.at(borough);
#pragma omp atomic
            local_boroughWeekAcc[brghIndex][year][week].numAccidents++;
#pragma omp atomic
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
        cout << "\nTotal contributing factors parsed: " << cfDictionary.size() << "\n\n\n";

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
        cout << "NUM. OMP THREADS:\t" << num_omp_threads << endl;

        double avgOverall,
            avgLoading, avgProcessing, avgWriting = 0;
        for (int i = 0; i < num_workers; i++)
        {
            cout << "Process {" << i << "}\t";
            cout << "Loading(" << loadTimes[i] << "), ";
            cout << "Scattering(" << scatterTimes[i] << "), ";
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
             << "[1] Loading(" << avgLoading / num_workers << "), "
             << "[2] Processing(" << avgProcessing / num_workers << "), "
             //  << "[3] Writing(" << avgWriting / num_workers << ") and \t"
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
