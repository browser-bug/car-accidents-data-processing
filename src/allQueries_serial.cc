#include <vector>
#include <map>
#include <string>

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

    // Support dictonaries
    map<string, int> cfDictionary;
    map<string, int> brghDictionary;

    // Local data structures
    vector<Row> localRows;
    int my_num_rows;

    // Timing stats variables
    double overallBegin, overallDuration = 0; // overall application duration time
    double loadBegin, loadDuration = 0;       // loading phase duration time
    double procBegin, procDuration = 0;       // processing phase duration time
    double writeBegin, writeDuration = 0;     // printing stats duration time

    int local_lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};                  // Local data structure for QUERY1
    AccPair local_accAndPerc[NUM_CONTRIBUTING_FACTORS] = {};                       // Local data structure for QUERY2
    AccPair local_boroughWeekAcc[NUM_BOROUGH][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {}; // Local data structure for QUERY3

    // Stats variables
    string statsFilePath = "../stats/stats_serial_" + dataset_dim + "_1p_1t.csv";
    Stats stats(0, NULL, 1, 1, statsFilePath);

    overallBegin = cpuSecond();

    // [1] Loading data from file
    loadBegin = cpuSecond();

    Loader loader(0, NULL, csv_path);
    loader.monoReadDataset(localRows);
    my_num_rows = localRows.size();

    cfDictionary = loader.getCFDict();
    brghDictionary = loader.getBRGHDict();

    loadDuration = cpuSecond() - loadBegin;
    stats.setLoadTimes(&loadDuration);

    // [2] Data processing
    procBegin = cpuSecond();

    cout << "Started processing dataset..." << endl;
    Process processer(0, NULL, &cfDictionary, &brghDictionary);

    // Every worker will compute in the final datastructure the num of lethal accidents for its sub-dataset and then Reduce it to allow the master to collect final results
    for (int i = 0; i < my_num_rows; i++)
    {
        processer.processQuery1(localRows[i], local_lethAccPerWeek);
        processer.processQuery2(localRows[i], local_accAndPerc);
        processer.processQuery3(localRows[i], local_boroughWeekAcc);
    }

    procDuration = cpuSecond() - procBegin;
    stats.setProcTimes(&procDuration);

    // [3] Output results
    writeBegin = cpuSecond();

    // Open output file
    const string outputDataPath = "../results/result_serial_" + dataset_dim + ".txt";
    Printer printer(0, NULL, outputDataPath, &cfDictionary, &brghDictionary);

    printer.openFile();

    printer.writeQuery1(local_lethAccPerWeek);
    printer.writeQuery2(local_accAndPerc);
    printer.writeQuery3(local_boroughWeekAcc);
    printer.closeFile();

    writeDuration = cpuSecond() - writeBegin;
    stats.setWriteTimes(&writeDuration);

    overallDuration = cpuSecond() - overallBegin;
    stats.setOverallTimes(&overallDuration);

    // Print statistics
    stats.openFile();
    stats.computeAverages();
    stats.printStats();
    stats.writeStats();
    stats.closeFile();
}
