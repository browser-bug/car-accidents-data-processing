#include "Process.h"

#include <string>

using namespace std;

void Process::processLethAccPerWeek(Row &data, int result[][NUM_WEEKS_PER_YEAR])
{
    if (data.isLethal())
    {
        auto date = computeWeekAndYear(data.date);
        result[date.year][date.week]++;
    }
}

void Process::processNumAccAndPerc(Row &data, AccPair result[NUM_BOROUGH])
{
    int cfIndex = 0;

    for (int k = 0; k < data.num_contributing_factors; k++)
    {
        cfIndex = dictQuery2.at(string(data.contributing_factors[k]));
        (result[cfIndex].numAccidents)++;
        if (data.isLethal())
            (result[cfIndex].numLethalAccidents)++;
    }
}

void Process::processBoroughWeekAcc(Row &data, AccPair result[][NUM_YEARS][NUM_WEEKS_PER_YEAR])
{
    int brghIndex = 0;
    string borough(data.borough);

    if (!borough.empty()) // if borough is not specified we're not interested
    {
        auto date = computeWeekAndYear(data.date);

        brghIndex = dictQuery3.at(borough);
        result[brghIndex][date.year][date.week].numAccidents++;
        if (data.isLethal())
            result[brghIndex][date.year][date.week].numLethalAccidents++;
    }
}

week_and_year Process::computeWeekAndYear(string date)
{
    week_and_year result;

    int w = getWeek(date), m = getMonth(date), y = getYear(date);

    // If I'm week = 1 and month = 12, this means I belong to the first week of the next year.
    // If I'm week = (52 or 53) and month = 01, this means I belong to the last week of the previous year.
    if (w == 1 && m == 12)
        y++;
    else if ((w == 52 || w == 53) && m == 1)
        y--;

    result.week = w - 1;
    result.year = y - BASE_YEAR;

    return result;
}