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

    map<string, pair<int, double>> AccAndPerc; // map { contributingFactor ; {numAccidents, percNumDeaths} }

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
        double lethal = (row.getNumPersonsKilled() > 0) ? 1 : 0;

        // For every CF found I insert it into the map
        for (auto itr = cfs.begin(); itr != cfs.end(); ++itr)
        {
            auto it = AccAndPerc.find(*itr);
            if (it != AccAndPerc.end())
            {
                (it->second.first)++; // increment num. accidents for this CF
                if (lethal)
                    (it->second.second)++; // increment num. of deaths for this CF
            }
            else
                AccAndPerc.insert({row[CONTRIBUTING_FACTOR_VEHICLE_1], {1, lethal}});
        }
    }

    double procDuration = cpuSecond() - procBegin;

    // [3] Output result
    double outBegin = cpuSecond();

    typedef map<string, pair<int, double>>::const_iterator MapIterator;
    for (MapIterator iter = AccAndPerc.begin(); iter != AccAndPerc.end(); iter++)
    {
        cout << iter->first << endl
             << "\t\tNum. of accidents: " << iter->second.first
             << "\t\t\tPerc. lethal accidents: " << setprecision(2) << fixed << (iter->second.second / iter->second.first) * 100 << "%"
             << endl;
    }

    double outDuration = cpuSecond() - outBegin;

    double overallDuration = cpuSecond() - overallBegin;

    cout << "Overall process duration is " << overallDuration << "s\n";
    cout << "It took " << loadDuration << "s to load the dataset\n";
    cout << "It took " << procDuration << "s to process the data\n";
    cout << "It took " << outDuration << "s to output the result\n";
}