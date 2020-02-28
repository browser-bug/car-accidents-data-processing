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
    string csv_path = "dataset/data_test.csv";
    ifstream file(csv_path);

    // // Testing on one single row
    // CSVRow row;
    // file >> row;
    // row.print();

    // // Print the entire dataset
    // CSVRow row;
    // for (CSVIterator loop(file); loop != CSVIterator(); loop++)
    // {
    //     row = (*loop);
    //     cout << endl;
    // }

    map<string, pair<int, int>> AccAndPerc; // map { contributingFactor ; {numAccidents, percNumDeaths} }

    for (CSVIterator loop(file); loop != CSVIterator(); loop++)
    {
        CSVRow row = (*loop);

        string cf = row[CONTRIBUTING_FACTOR_VEHICLE_1];
        int lethal = (row.getNumPersonKilled() > 0) ? 1 : 0;

        auto it = AccAndPerc.find(cf);
        if (it == AccAndPerc.end())
        {
            (it->second.first)++;
            if (lethal)
                (it->second.second)++;
        }
        else
            AccAndPerc.insert({row[CONTRIBUTING_FACTOR_VEHICLE_1], {1, lethal}});

        cout << endl;
    }

    typedef map<string, pair<int, int>>::const_iterator MapIterator;
    for (MapIterator iter = AccAndPerc.begin(); iter != AccAndPerc.end(); iter++)
    {
        cout << iter->first << "\t\t\t Num. of accidents: ";
        cout << iter->second.first;
        cout << "\t Perc. lethal accidents: ";
        cout << (iter->second.second / iter->second.first) << endl;
    }
}