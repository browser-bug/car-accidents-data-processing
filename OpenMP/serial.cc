#include <omp.h>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>
#include <iomanip>

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

    // Number of lethal accidents per week throughout the entire dataset. Using index: NUMBER_OF_PERSONS_KILLED
    for (CSVIterator loop(file); loop != CSVIterator(); ++loop)
    {
        CSVRow row = (*loop);
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

    int year = 2016; // Choose year to print
    typedef map<int, int>::const_iterator MapIterator;
    for (MapIterator iter = accPerWeek[year - 2012].begin(); iter != accPerWeek[year - 2012].end(); iter++)
    {
        cout << "(" << year << ")Week: " << iter->first << "\t\t\t Num. person killed: ";
        cout << iter->second << endl;
    }
}