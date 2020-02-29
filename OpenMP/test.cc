#include <omp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <list>
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
    // file >> row >> row;
    // row.print();
    // cout << endl;
    // vector<string> cfs = row.getContributingFactors();
    // for (auto it = cfs.begin(); it != cfs.end(); ++it)
    //     cout << *it << "\t";
    // cout << endl;
    // cout << row[UNIQUE_KEY];

    // // Print the entire dataset
    // for (CSVIterator loop(file); loop != CSVIterator(); loop++)
    // {
    //     CSVRow row = (*loop);
    //     row.print();
    //     cout << endl;
    // }

    /* PRINT ALL WEEKS FOR EACH YEAR */
    // map<int, list<string>> yearWeeks;
    // int whichYear = 2017; // specify which year you want
    // CSVRow row;
    // CSVIterator loop(file);
    // loop++; // skip the header
    // for (; loop != CSVIterator(); loop++)
    // {
    //     row = (*loop);
    //     int year = getYear(row[DATE]);
    //     if (year != whichYear)
    //         continue;
    //     int week = getWeek(row[DATE]);
    //     int month = getMonth(row[DATE]);

    //     // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
    //     // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
    //     if (week == 1 && month == 12)
    //     {
    //         year++;
    //         continue;
    //     }
    //     else if ((week == 52 || week == 53) && month == 1)
    //     {
    //         year--;
    //         continue;
    //     }

    //     auto it = yearWeeks.find(week);
    //     if (it != yearWeeks.end())
    //     {
    //         if (find(it->second.begin(), it->second.end(), row[DATE]) == it->second.end())
    //             it->second.push_back(row[DATE]);
    //     }
    //     else
    //         yearWeeks.insert({week, {row[DATE]}});
    // }

    // // print weeks
    // for (auto iter = yearWeeks.begin(); iter != yearWeeks.end(); ++iter)
    // {
    //     cout << "Week (" << iter->first << ") ";
    //     // print weeks
    //     for (auto it = iter->second.begin(); it != iter->second.end(); ++it)
    //     {
    //         cout << *it << "\t";
    //     }
    //     cout << endl;
    // }
}