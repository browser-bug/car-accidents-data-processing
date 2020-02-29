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

    // map { weekNumber ; numLethalAccidents }
    vector<map<int, int>> lethAccPerWeek = {
        map<int, int>(), // 2012
        map<int, int>(), // 2013
        map<int, int>(), // 2014
        map<int, int>(), // 2015
        map<int, int>(), // 2016
        map<int, int>()  // 2017
    };

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
        int num_pers_killed = row.getNumPersonsKilled();
        int week = getWeek(row[DATE]);
        int month = getMonth(row[DATE]);
        int year = getYear(row[DATE]) - 2012;

        // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
        // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
        if (week == 1 && month == 12)
            year++;
        else if ((week == 52 || week == 53) && month == 1)
            year--;

        if (num_pers_killed > 0) // We have a lethal accident
        {
            auto it = lethAccPerWeek[year].find(week);
            if (it != lethAccPerWeek[year].end())
                (it->second)++;
            else
                lethAccPerWeek[year].insert({week, 1});
        }
    }

    double procDuration = cpuSecond() - procBegin;

    // [3] Output result
    double outBegin = cpuSecond();

    int totalWeeks = 0;
    int totalAccidents = 0;
    typedef map<int, int>::const_iterator MapIterator;
    for (unsigned int i = 0; i < lethAccPerWeek.size(); ++i)
    {
        for (MapIterator iter = lethAccPerWeek[i].begin(); iter != lethAccPerWeek[i].end(); iter++)
        {
            cout << "(" << (i + 2012) << ")Week: " << iter->first << "\t\t\t Num. lethal accidents: ";
            cout << iter->second << endl;
            totalAccidents += iter->second;
        }
        totalWeeks += lethAccPerWeek[i].size();
    }

    double outDuration = cpuSecond() - outBegin;

    double overallDuration = cpuSecond() - overallBegin;

    cout << "Overall process duration is " << overallDuration << "s\n";
    cout << "It took " << loadDuration << "s to load the dataset\n";
    cout << "It took " << procDuration << "s to process the data\n";
    cout << "It took " << outDuration << "s to output the result\n";
    cout << "Total weeks: " << totalWeeks << endl;
    cout << "Total accidents: " << totalAccidents << endl;
}