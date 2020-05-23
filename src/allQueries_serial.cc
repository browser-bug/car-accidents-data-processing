#include <vector>
#include <map>
#include <string>

#include "utils/csv_row/include/CSVIterator.h"
#include "utils/include/utility.h"
#include "utils/include/data_types.h"

#include "Loader.h"
#include "Communicator.h"
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

    // Load dataset variables
    const string dataset_dir_path = "../dataset/";
    const string csv_path = dataset_dir_path + "collisions_" + dataset_dim + ".csv";

    // Results data
    int *global_lethAccPerWeek;     // Global data structure for QUERY1
    AccPair *global_accAndPerc;     // Global data structure for QUERY2
    AccPair *global_boroughWeekAcc; // Global data structure for QUERY3

    // Support dictonaries
    map<string, int> cfDictionary;
    map<string, int> brghDictionary;

    // Local data structures
    vector<Row> localRows;
    int my_num_rows;
    int numYears, numWeeksPerYear;
    int numContributingFactors, numBorough;

    // Timing stats variables
    double overallBegin = 0, overallDuration = 0; // overall application duration time
    double loadBegin = 0, loadDuration = 0;       // loading phase duration time
    double procBegin = 0, procDuration = 0;       // processing phase duration time
    double writeBegin = 0, writeDuration = 0;     // printing stats duration time

    // Stats variables
    const string stats_dir_path = "../stats/";
    const string stats_path = stats_dir_path + "stats_serial_" + dataset_dim + "_1p_1t.csv";
    Stats stats(stats_path);

    overallBegin = cpuSecond();

    // [1] Loading data from file
    loadBegin = cpuSecond();

    Loader loader(csv_path);
    loader.monoReadDataset(localRows);
    my_num_rows = localRows.size();

    cfDictionary = loader.getCFDict();
    brghDictionary = loader.getBRGHDict();

    loadDuration = cpuSecond() - loadBegin;
    stats.setLoadTimes(&loadDuration);

    // [2] Data processing
    procBegin = cpuSecond();

    /* Retrieving #elements for each query */
    numYears = loader.getNumYears();
    numWeeksPerYear = NUM_WEEKS_PER_YEAR;
    numContributingFactors = cfDictionary.size();
    numBorough = brghDictionary.size();

    /* Allocating data */
    global_lethAccPerWeek = new int[numYears * numWeeksPerYear]();
    global_accAndPerc = new AccPair[numContributingFactors]();
    global_boroughWeekAcc = new AccPair[numBorough * numYears * numWeeksPerYear]();

    cout << "Started processing dataset..." << endl;
    Process processer(numYears, numWeeksPerYear, &cfDictionary, &brghDictionary);

    for (int i = 0; i < my_num_rows; i++)
    {
        processer.processLethAccPerWeek(localRows[i], global_lethAccPerWeek);
        processer.processNumAccAndPerc(localRows[i], global_accAndPerc);
        processer.processBoroughWeekAcc(localRows[i], global_boroughWeekAcc);
    }

    procDuration = cpuSecond() - procBegin;
    stats.setProcTimes(&procDuration);

    // [3] Output results
    writeBegin = cpuSecond();

    // Open output file
    const string result_dir_path = "../results/";
    const string result_path = result_dir_path + "result_serial_" + dataset_dim + ".txt";
    Printer printer(result_path, numYears, numWeeksPerYear, &cfDictionary, &brghDictionary);

    if (!printer.openFile())
    {
        cout << result_path << ": No such file or directory" << endl;
        goto exit;
    }
    printer.writeLethAccPerWeek(global_lethAccPerWeek);
    printer.writeNumAccAndPerc(global_accAndPerc);
    printer.writeBoroughWeekAcc(global_boroughWeekAcc);

    printer.closeFile();

    writeDuration = cpuSecond() - writeBegin;
    stats.setWriteTimes(&writeDuration);

    overallDuration = cpuSecond() - overallBegin;
    stats.setOverallTimes(&overallDuration);

    // Print statistics
    if (!stats.openFile())
    {
        cout << stats_path << ": No such file or directory" << endl;
        goto exit;
    }

    stats.computeAverages();
    stats.printStats();
    stats.writeStats();

    stats.closeFile();

exit:
    // Deallocate memory
    delete[] global_lethAccPerWeek;
    delete[] global_accAndPerc;
    delete[] global_boroughWeekAcc;

    return 0;
}