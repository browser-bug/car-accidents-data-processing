#include <vector>
#include <map>
#include <string>
#include <math.h>

#include "utils/csv_row/include/CSVIterator.h"
#include "utils/include/utility.h"
#include "utils/include/data_types.h"

#include "Loader.h"
#include "Scatterer.h"
#include "Process.h"
#include "Printer.h"
#include "Stats.h"

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
    const string dataset_path = "../dataset/";
    const string csv_path = dataset_path + "collisions_" + dataset_dim + ".csv";
    int csv_size = 0;

    // Support dictonaries
    map<string, int> cfDictionary;
    map<string, int> brghDictionary;

    // MPI variables
    int myrank, num_workers;

    int global_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};                 // Global data structure for QUERY1
    AccPair global_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};                      // Global data structure for QUERY2
    AccPair global_boroughWeekAc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {}; // Global data structure for QUERY3

    // MPI Datatypes definitions
    MPI_Datatype rowType;

    // MPI Operators definitions
    MPI_Op accPairSum;

    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);

    MPI_Type_create_struct(5, rowLength, rowDisplacements, rowTypes, &rowType);
    MPI_Type_commit(&rowType);

    MPI_Op_create(pairSum, true, &accPairSum);

    // Local data structures
    vector<Row> dataScatter;
    vector<Row> localRows;
    int my_num_rows;

    // Timing stats variables
    double overallBegin, overallDuration = 0; // overall application duration time
    double loadBegin, loadDuration = 0;       // loading phase duration time
    double scatterBegin, scatterDuration = 0; // scattering phase duration time
    double procBegin, procDuration = 0;       // processing phase duration time
    double writeBegin, writeDuration = 0;     // printing stats duration time

    int local_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};                  // Local data structure for QUERY1
    AccPair local_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};                       // Local data structure for QUERY2
    AccPair local_boroughWeekAcc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {}; // Local data structure for QUERY3

    // Stats variables
    string statsFilePath = "../stats/stats_monoread_" + dataset_dim + "_" + to_string(num_workers) + "p_" + to_string(num_omp_threads) + "t.csv";
    Stats stats(myrank, MPI_COMM_WORLD, num_workers, num_omp_threads, statsFilePath);

    overallBegin = MPI_Wtime();

    // [1] Loading data from file

    if (myrank == 0)
    {
        loadBegin = MPI_Wtime();
        Loader loader(myrank, MPI_COMM_WORLD, csv_path);
        loader.monoReadDataset(dataScatter);
        csv_size = dataScatter.size();

        cfDictionary = loader.getCFDict();
        brghDictionary = loader.getBRGHDict();
        loadDuration = MPI_Wtime() - loadBegin;
    }

    stats.setLoadTimes(&loadDuration);

    MPI_Barrier(MPI_COMM_WORLD); /* wait for master to complete reading */

    // Initialization for scattering, evenly dividing dataset

    scatterBegin = MPI_Wtime();

    Scatterer scatterer(myrank, MPI_COMM_WORLD, num_workers);
    // Broadcasting the dictionaries to all processes
    scatterer.broadcastDictionary(cfDictionary, MAX_CF_LENGTH);
    scatterer.broadcastDictionary(brghDictionary, MAX_BOROUGH_LENGTH);

    scatterer.scatterData(&dataScatter, &localRows, rowType, csv_size);
    my_num_rows = localRows.size();

    scatterDuration = MPI_Wtime() - scatterBegin;
    stats.setScatterTimes(&scatterDuration);

    // [2] Data processing
    procBegin = MPI_Wtime();

    cout << "[Proc. " + to_string(myrank) + "] Started processing dataset..." << endl;
    int dynChunk = (int)round(my_num_rows * 0.02); // this tunes the chunk size exploited by dynamic scheduling based on percentage
    Process processer(myrank, MPI_COMM_WORLD, &cfDictionary, &brghDictionary);

    omp_set_num_threads(num_omp_threads);

#pragma omp declare reduction(accPairSum:AccPair \
                              : omp_out += omp_in)

// Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
#pragma omp parallel for default(shared) schedule(dynamic, dynChunk) reduction(+                                            \
                                                                               : local_lethAccPerWeek) reduction(accPairSum \
                                                                                                                 : local_accAndPerc, local_boroughWeekAcc)
    for (int i = 0; i < my_num_rows; i++)
    {
        processer.processQuery1(localRows[i], local_lethAccPerWeek);
        processer.processQuery2(localRows[i], local_accAndPerc);
        processer.processQuery3(localRows[i], local_boroughWeekAcc);
    }

    // Query1
    MPI_Reduce(local_lethAccPerWeek, global_lethAccPerWeek, (NUM_YEARS * NUM_WEEKS_PER_YEAR), MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    // Query2
    MPI_Reduce(local_accAndPerc, global_accAndPerc, NUM_CONTRIBUTING_FACTORS, MPI_2INT, accPairSum, 0, MPI_COMM_WORLD);
    // Query3
    MPI_Reduce(local_boroughWeekAcc, global_boroughWeekAc, NUM_BOROUGH * NUM_YEARS * NUM_WEEKS_PER_YEAR, MPI_2INT, accPairSum, 0, MPI_COMM_WORLD);

    procDuration = MPI_Wtime() - procBegin;
    stats.setProcTimes(&procDuration);

    // [3] Output results

    if (myrank == 0)
    {
        writeBegin = MPI_Wtime();
        // Open output file
        const string outputDataPath = "../results/result_monoread_" + dataset_dim + ".txt";
        Printer printer(myrank, MPI_COMM_WORLD, outputDataPath, &cfDictionary, &brghDictionary);

        printer.openFile();

        printer.writeQuery1(global_lethAccPerWeek);
        printer.writeQuery2(global_accAndPerc);
        printer.writeQuery3(global_boroughWeekAc);

        printer.closeFile();
        writeDuration = MPI_Wtime() - writeBegin;
    }

    stats.setWriteTimes(&writeDuration);

    overallDuration = MPI_Wtime() - overallBegin;
    stats.setOverallTimes(&overallDuration);

    // Print statistics
    if (myrank == 0)
    {
        stats.openFile();
        stats.computeAverages();
        stats.printStats();
        stats.writeStats();
        stats.closeFile();
    }

    MPI_Finalize();
}
