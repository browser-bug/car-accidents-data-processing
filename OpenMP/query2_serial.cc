#include <omp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>

#include "../utilities/CSVIterator.h"

#define NUM_CONTRIBUTING_FACTORS 47

using namespace std;

int main()
{
    // complete dataset NYPD_Motor_Vehicle_Collisions
    string csv_path = "../dataset/NYPD_Motor_Vehicle_Collisions.csv";
    ifstream file(csv_path);

    // map { contributingFactor ; {numAccidents, numLethalAccidents} }
    map<string, pair<int, int>> accAndPerc;

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

        vector<string> cfs = row.getContributingFactors();
        if (cfs.empty())
            continue;
        int lethal = (row.getNumPersonsKilled() > 0) ? 1 : 0;

        // For every CF found I insert it into the map
        for (auto itr = cfs.begin(); itr != cfs.end(); ++itr)
        {
            auto it = accAndPerc.find(*itr);
            if (it != accAndPerc.end())
            {
                (it->second.first)++;          // increment num. accidents for this CF
                (it->second.second) += lethal; // increment num. lethal accidents for this CF
            }
            else
                accAndPerc.insert({*itr, {1, lethal}});
        }
    }

    double procDuration = cpuSecond() - procBegin;

    // [3] Output result
    double outBegin = cpuSecond();

    typedef map<string, pair<int, int>>::const_iterator MapIterator;
    for (MapIterator iter = accAndPerc.begin(); iter != accAndPerc.end(); iter++)
    {
        double perc = double(iter->second.second) / iter->second.first;
        cout << iter->first << endl
             << "\t\tNum. of accidents: " << iter->second.first
             << "\t\t\tPerc. lethal accidents: " << setprecision(2) << fixed << perc * 100 << "%"
             << endl;
    }

    double outDuration = cpuSecond() - outBegin;

    double overallDuration = cpuSecond() - overallBegin;

    // Print statistics
    cout << fixed << setprecision(8) << endl;
    cout << "Overall process duration is " << overallDuration << "s\n";
    cout << "It took " << loadDuration << "s to load the dataset\n";
    cout << "It took " << procDuration << "s to process the data\n";
    cout << "It took " << outDuration << "s to output the result\n";
}