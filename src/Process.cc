#include "Process.h"

#include <string>

using namespace std;

void Process::processQuery1(const Row &data, int result[][NUM_WEEKS_PER_YEAR])
{
    int week, year = 0;
    int lethal = (data.num_pers_killed > 0) ? 1 : 0;

    if (lethal)
    {
        computeWeekAndYear(data.date, week, year);

        result[year][week]++;
    }
}

void Process::processQuery2(const Row &data, AccPair result[NUM_BOROUGH])
{
    int cfIndex = 0;
    int lethal = (data.num_pers_killed > 0) ? 1 : 0;

    for (int k = 0; k < data.num_contributing_factors; k++)
    {
        cfIndex = dictQuery2.at(string(data.contributing_factors[k]));
        (result[cfIndex].numAccidents)++;
        (result[cfIndex].numLethalAccidents) += lethal;
    }
}

void Process::processQuery3(const Row &data, AccPair result[][NUM_YEARS][NUM_WEEKS_PER_YEAR])
{
    int week, year = 0;
    int brghIndex = 0;
    int lethal = (data.num_pers_killed > 0) ? 1 : 0;
    string borough = string(data.borough);

    if (!borough.empty()) // if borough is not specified we're not interested
    {
        computeWeekAndYear(data.date, week, year);

        brghIndex = dictQuery3.at(borough);
        result[brghIndex][year][week].numAccidents++;
        result[brghIndex][year][week].numLethalAccidents += lethal;
    }
}

void Process::computeWeekAndYear(string date, int &week, int &year)
{
    int w, m, y = 0;

    w = getWeek(date);
    m = getMonth(date);
    y = getYear(date);

    // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
    // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
    if (w == 1 && m == 12)
        y++;
    else if ((w == 52 || w == 53) && m == 1)
        y--;

    week = w - 1;
    year = y - 2012;
}
