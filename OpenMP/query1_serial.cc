#include <omp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>

#include "utilities/CSVIterator.h"

using namespace std;

int main()
{
    // complete dataset NYPD_Motor_Vehicle_Collisions
    string csv_path = "dataset/NYPD_Motor_Vehicle_Collisions.csv";
    ifstream file(csv_path);

    // map { { year, week }, numLethalAccidents }
    typedef pair<int, int> yearWeekKey;
    map<yearWeekKey, int> lethAccPerWeek;

    double overallBegin = cpuSecond();

    // [1] Loading data from file
    double loadBegin = cpuSecond();

    // CSVRow* dataset = new CSVRow[955928];
    vector<CSVRow> dataset;
    CSVIterator loop(file);
    ++loop; // skip the header
    for (; loop != CSVIterator(); ++loop)
        dataset.push_back((*loop));

    double loadDuration = cpuSecond() - loadBegin;

    // [2] Data processing
    double procBegin = cpuSecond();

    for (unsigned int i = 0; i < dataset.size(); ++i)
    {
        CSVRow row = dataset[i];
        int lethal = (row.getNumPersonsKilled() > 0) ? 1 : 0;
        int week = getWeek(row[DATE]);
        int month = getMonth(row[DATE]);
        int year = getYear(row[DATE]);

        // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
        // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
        if (week == 1 && month == 12)
            year++;
        else if ((week == 52 || week == 53) && month == 1)
            year--;

        auto it = lethAccPerWeek.find({year, week});
        if (it != lethAccPerWeek.end() && lethal)
            (it->second)++;
        else
            lethAccPerWeek.insert({{year, week}, lethal});
    }

    double procDuration = cpuSecond() - procBegin;

    // [3] Output result
    double outBegin = cpuSecond();

    int totalWeeks = 0;
    int totalAccidents = 0;
    typedef map<yearWeekKey, int>::const_iterator MapIterator;
    for (auto iter = lethAccPerWeek.begin(); iter != lethAccPerWeek.end(); iter++)
    {
        cout << "(" << iter->first.first << ")Week: " << iter->first.second << "\t\t\t Num. lethal accidents: ";
        cout << iter->second << endl;
        totalAccidents += iter->second;
        totalWeeks++;
    }
    cout << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << endl;

    double outDuration = cpuSecond() - outBegin;

    double overallDuration = cpuSecond() - overallBegin;

    cout << endl;
    cout << "Overall process duration is " << overallDuration << "s\n";
    cout << "It took " << loadDuration << "s to load the dataset\n";
    cout << "It took " << procDuration << "s to process the data\n";
    cout << "It took " << outDuration << "s to output the result\n";
}