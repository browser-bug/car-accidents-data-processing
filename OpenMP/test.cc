#include <omp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <string>

#include "../utilities/CSVIterator.h"

#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

#define NUM_CONTRIBUTING_FACTORS 47

using namespace std;

int main()
{
    bool testing = true; // switch between dataset for testing and original dataset

    string csv_path = testing ? "../dataset/data_test.csv" : "../dataset/NYPD_Motor_Vehicle_Collisions.csv";
    ifstream file(csv_path);

    // Support dictonaries
    int indexCF = 0;
    map<string, int> contributingFactors;
    int indexB = 0;
    map<string, int> boroughs;

    // Loading the dataset
    int csv_size = testing ? TEST_SIZE : ORIGINAL_SIZE;

    CSVRow *dataset = new CSVRow[csv_size];
    for (int i = 0; i < csv_size; i++)
    {
        if (i == 0)
            file >> dataset[0] >> dataset[0]; // skip the header
        else
            file >> dataset[i];

        // Creating dictonary for query 2
        string cf = (dataset[i])[CONTRIBUTING_FACTOR_VEHICLE_1];
        if (!cf.empty() && cf.compare("Unspecified") && contributingFactors.find(cf) == contributingFactors.end())
            contributingFactors.insert({cf, indexCF++});

        // Creating dictonary for query 3
        string b = (dataset[i])[BOROUGH];
        if (!b.empty() && b.compare("Unspecified") && boroughs.find(b) == boroughs.end())
            boroughs.insert({b, indexB++});
    }

    /////////////
    /* QUERY 1 */
    /////////////

    // // years [2012, 2013, 2014, 2015, 2016, 2017]
    // int lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    // for (int i = 0; i < csv_size; ++i)
    // {
    //     CSVRow row = dataset[i];
    //     int lethal = (row.getNumPersonsKilled() > 0) ? 1 : 0;
    //     int week = getWeek(row[DATE]);
    //     int month = getMonth(row[DATE]);
    //     int year = getYear(row[DATE]);

    //     // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
    //     // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
    //     if (week == 1 && month == 12)
    //         year++;
    //     else if ((week == 52 || week == 53) && month == 1)
    //         year--;

    //     if (lethal)
    //         lethAccPerWeek[year - 2012][week - 1]++;
    // }

    // int totalWeeks = 0;
    // int totalAccidents = 0;

    // for (int year = 0; year < NUM_YEARS; year++)
    // {
    //     for (int week = 0; week < NUM_WEEKS_PER_YEAR; week++)
    //     {
    //         int numLethAcc = lethAccPerWeek[year][week];
    //         if (numLethAcc > 0)
    //         {
    //             cout << "(" << (year + 2012) << ")Week: " << (week + 1) << "\t\t\t Num. lethal accidents: ";
    //             cout << numLethAcc << endl;
    //             totalAccidents += numLethAcc;
    //             totalWeeks++;
    //         }
    //     }
    // }
    // cout << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << endl;

    /////////////
    /* QUERY 2 */
    /////////////

    // // pair = {numAccidents, numLethalAccidents}
    // int num_contributing_factors = contributingFactors.size();
    // pair<int, int> accAndPerc[num_contributing_factors] = {};

    // for (int i = 0; i < csv_size; ++i)
    // {
    //     CSVRow row = dataset[i];

    //     vector<string> cfs = row.getContributingFactors();
    //     if (cfs.empty())
    //         continue;
    //     int lethal = (row.getNumPersonsKilled() > 0) ? 1 : 0;

    //     // For every CF found I insert it into the map
    //     for (auto cf : cfs)
    //     {
    //         accAndPerc[contributingFactors[cf]].first++;          // increment num. accidents for this CF
    //         accAndPerc[contributingFactors[cf]].second += lethal; // increment num. lethal accidents for this CF
    //     }
    // }

    // for (auto el : contributingFactors)
    // {
    //     double perc = double(accAndPerc[el.second].second) / accAndPerc[el.second].first;
    //     cout << el.first << endl
    //          << "\t\tNum. of accidents: " << accAndPerc[el.second].first
    //          << "\t\t\tPerc. lethal accidents: " << setprecision(2) << fixed << perc * 100 << "%"
    //          << endl;
    // }

    /////////////
    /* QUERY 3 */
    /////////////

    // pair = { numAccidents, numLethalAccidents }
    int numBoroughs = boroughs.size();
    pair<int, int> boroughWeekAcc[numBoroughs][NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    for (int i = 0; i < csv_size; ++i)
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

        int borIndex = boroughs[borough];
        boroughWeekAcc[borIndex][year - 2012][week - 1].first++;
        boroughWeekAcc[borIndex][year - 2012][week - 1].second += lethal;
    }

    for (auto b : boroughs) // for each borough
    {
        int numWeeks = 0;
        int numAccidents = 0;
        double numLethalAccidents = 0;

        cout << "Borough: " << b.first << endl;
        for (int i = 0; i < NUM_YEARS; i++) // for each year
        {
            for (int j = 0; j < NUM_WEEKS_PER_YEAR; j++) // for each week
            {
                if (boroughWeekAcc[b.second][i][j].first == 0)
                    continue;
                numWeeks++;
                numAccidents += boroughWeekAcc[b.second][i][j].first;
                numLethalAccidents += boroughWeekAcc[b.second][i][j].second;

                cout << "(" << (i + 2012) << ")Week " << (j + 1);                                  // print (Year)Week N
                cout << "\t\t\t num. accidents: " << boroughWeekAcc[b.second][i][j].first << endl; // print numAccidents
            }
        }
        double avg = numLethalAccidents / numWeeks;
        cout << "[" << b.first << "] Avg. lethal accidents per week is: " << setprecision(2) << fixed << avg * 100 << "%";
        cout << "\t\t\tNum. accidents: " << numAccidents << "\t\tNum. lethal accidents: " << setprecision(0) << fixed << numLethalAccidents << endl;
        cout << endl;
    }

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

    // vector<string> borough;

    // // Print the entire dataset
    // CSVIterator loop(file);
    // loop++; // skip header
    // for (; loop != CSVIterator(); loop++)
    // {
    //     CSVRow row = (*loop);
    //     if (find(borough.begin(), borough.end(), row[BOROUGH]) == borough.end() && !row[BOROUGH].empty())
    //         borough.push_back(row[BOROUGH]);
    // }

    // cout << "Boroughs : " << endl;
    // for (auto it = borough.begin(); it != borough.end(); ++it)
    // {
    //     cout << *it << endl;
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