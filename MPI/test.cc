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

    // LOADING THE DATASET
    int csv_size = testing ? TEST_SIZE : ORIGINAL_SIZE;

    // Support dictonaries
    int indexCF = 0;
    map<string, int> contributingFactors;
    int indexB = 0;
    map<string, int> boroughs;

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

    // years [2012, 2013, 2014, 2015, 2016, 2017]
    int lethAccPerWeek[NUM_YEARS][NUM_WEEKS_PER_YEAR] = {};

    
    for (int i = 0; i < csv_size; ++i)
    {
        CSVRow row = dataset[i];
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

        if (lethal)
            lethAccPerWeek[year - 2012][week - 1]++;
    }

    int totalWeeks = 0;
    int totalAccidents = 0;

    for (int year = 0; year < NUM_YEARS; year++)
    {
        for (int week = 0; week < NUM_WEEKS_PER_YEAR; week++)
        {
            int numLethAcc = lethAccPerWeek[year][week];
            if (numLethAcc > 0)
            {
                cout << "(" << (year + 2012) << ")Week: " << (week + 1) << "\t\t\t Num. lethal accidents: ";
                cout << numLethAcc << endl;
                totalAccidents += numLethAcc;
                totalWeeks++;
            }
        }
    }
    cout << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << endl;
}