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

    // // Testing on one single row
    // CSVRow row;
    // file >> row;
    // row.print();
    // cout << endl;
    // vector<string> cfs = row.getContributingFactors();
    // cout << cfs.size() << endl;
    // for (auto it = cfs.begin(); it != cfs.end(); ++it)
    //     cout << *it << endl;

    // // Print the entire dataset
    // for (CSVIterator loop(file); loop != CSVIterator(); loop++)
    // {
    //     CSVRow row;
    //     row = (*loop);
    //     row.print();
    //     cout << endl;
    // }

    map<string, pair<int, double>> AccAndPerc; // map { contributingFactor ; {numAccidents, percNumDeaths} }

    for (CSVIterator loop(file); loop != CSVIterator(); loop++)
    {
        CSVRow row = (*loop);

        vector<string> cfs = row.getContributingFactors();
        if (cfs.empty())
            continue;
        double lethal = (row.getNumPersonKilled() > 0) ? 1 : 0;

        // For every CF found I insert it into the map
        for (auto itr = cfs.begin(); itr != cfs.end(); ++itr)
        {
            auto it = AccAndPerc.find(*itr);
            if (it != AccAndPerc.end())
            {
                (it->second.first)++;
                if (lethal)
                    (it->second.second)++;
            }
            else
                AccAndPerc.insert({row[CONTRIBUTING_FACTOR_VEHICLE_1], {1, lethal}});
        }
    }

    typedef map<string, pair<int, double>>::const_iterator MapIterator;
    for (MapIterator iter = AccAndPerc.begin(); iter != AccAndPerc.end(); iter++)
    {
        cout << iter->first << endl
             << "\t\tNum. of accidents: " << iter->second.first
             << "\t\t\tPerc. lethal accidents: " << (iter->second.second / iter->second.first)
             << endl;
    }
}