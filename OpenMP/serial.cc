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
    string csv_path = "dataset/NYPD_Motor_Vehicle_Collisions.csv";
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
    //     row.print();
    //     cout << endl;
    // }

    // map { weekNumber ; num_accidents }
    vector<map<int, int>> accPerWeek = {
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

    vector<CSVRow> dataset;
    for (CSVIterator loop(file); loop != CSVIterator(); ++loop)
        dataset.push_back((*loop));

    double loadDuration = cpuSecond() - loadBegin;
    printf("It took %fs to load the dataset\n", loadDuration);

    // [2] Data processing
    double procBegin = cpuSecond();

    for (unsigned int i = 0; i < dataset.size(); ++i)
    {
        CSVRow row = dataset[i];
        int num_pers_killed = row.getNumPersonKilled();
        int week = getWeek(row[DATE]);
        int year = getYear(row[DATE]) - 2012;

        if (num_pers_killed > 0)
        {
            auto it = accPerWeek[year].find(week);
            if (it != accPerWeek[year].end())
                it->second += num_pers_killed;
            else
                accPerWeek[year].insert({week, num_pers_killed});
        }
    }

    double procDuration = cpuSecond() - procBegin;
    printf("It took %fs to process the data\n", procDuration);

    // [3] Output result
    double outBegin = cpuSecond();

    typedef map<int, int>::const_iterator MapIterator;
    for (unsigned int i = 0; i < accPerWeek.size(); ++i)
    {
        for (MapIterator iter = accPerWeek[i].begin(); iter != accPerWeek[i].end(); iter++)
        {
            cout << "(" << (i + 2012) << ")Week: " << iter->first << "\t\t\t Num. person killed: ";
            cout << iter->second << endl;
        }
    }

    double outDuration = cpuSecond() - outBegin;
    printf("It took %fs to output the result\n", outDuration);

    double overallDuration = cpuSecond() - overallBegin;
    printf("Overall process duration is %fs", overallDuration);
}