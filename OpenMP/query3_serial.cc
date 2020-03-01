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

    // map { borough ; { { year, week } ; { numAccidents, numLethalAccidents } } }
    typedef pair<int, int> yearWeekKey;
    typedef map<yearWeekKey, pair<int, int>> innerMap;
    map<string, innerMap> boroughWeekAcc;

    double overallBegin = cpuSecond();

    // [1] Loading data from file
    double loadBegin = cpuSecond();

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

        string borough = row[BOROUGH];
        if (borough.empty()) // if borough is not specified we're not interested
            continue;

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

        auto boroughItr = boroughWeekAcc.find(borough);
        if (boroughItr != boroughWeekAcc.end()) // check if borough is present
        {
            auto yearWeekItr = boroughItr->second.find({year, week});
            if (yearWeekItr != boroughItr->second.end()) // check if {year,week} is present
            {
                (yearWeekItr->second.first)++;          // increase numAccidents
                (yearWeekItr->second.second) += lethal; // increase numLethalAccidents if lethal
            }
            else
            {
                boroughItr->second.insert({make_pair(year, week), make_pair(1, lethal)});
            }
        }
        else
        {
            boroughWeekAcc.insert({borough, {{make_pair(year, week), make_pair(1, lethal)}}});
        }
    }

    double procDuration = cpuSecond() - procBegin;

    // [3] Output result
    double outBegin = cpuSecond();

    typedef map<pair<int, int>, pair<int, int>> inMap;
    typedef map<string, inMap> outerMap;
    for (outerMap::iterator it1 = boroughWeekAcc.begin(); it1 != boroughWeekAcc.end(); it1++) // for each borough
    {
        int numWeeks = 0;
        int numAccidents = 0;
        double numLethalAccidents = 0;

        cout << "Borough: " << it1->first << endl;
        for (inMap::iterator it2 = it1->second.begin(); it2 != it1->second.end(); it2++)
        {
            numWeeks++;
            numAccidents += it2->second.first;
            numLethalAccidents += it2->second.second;

            cout << "(" << it2->first.first << ")Week " << it2->first.second; // print (Year)Week N
            cout << "\t\t\t num. accidents: " << it2->second.first << endl;   // print numAccidents
        }
        double avg = numLethalAccidents / numWeeks;
        cout << "[" << it1->first << "] Avg. lethal accidents per week is: " << setprecision(2) << fixed << avg * 100 << "%";
        cout << "\t\t\tNum. accidents: " << numAccidents << "\t\tNum. lethal accidents: " << setprecision(0) << fixed << numLethalAccidents << endl;
        cout << endl;
    }

    double outDuration = cpuSecond() - outBegin;

    double overallDuration = cpuSecond() - overallBegin;

    cout << "Overall process duration is " << overallDuration << "s\n";
    cout << "It took " << loadDuration << "s to load the dataset\n";
    cout << "It took " << procDuration << "s to process the data\n";
    cout << "It took " << outDuration << "s to output the result\n";
}