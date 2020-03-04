#include "utility.h"

using namespace std;

int getWeek(struct tm *date)
{
    if (NULL == date)
    {
        return 0; // or -1 or throw exception
    }
    if (::mktime(date) < 0) // Make sure _USE_32BIT_TIME_T is NOT defined.
    {
        return 0; // or -1 or throw exception
    }
    // The basic calculation:
    // {Day of Year (1 to 366) + 10 - Day of Week (Mon = 1 to Sun = 7)} / 7
    int monToSun = (date->tm_wday == 0) ? 7 : date->tm_wday; // Adjust zero indexed week day
    int week = ((date->tm_yday + 11 - monToSun) / 7);        // Add 11 because yday is 0 to 365.

    // Now deal with special cases:
    // A) If calculated week is zero, then it is part of the last week of the previous year.
    if (week == 0)
    {
        // We need to find out if there are 53 weeks in previous year.
        // Unfortunately to do so we have to call mktime again to get the information we require.
        // Here we can use a slight cheat - reuse this function!
        // (This won't end up in a loop, because there's no way week will be zero again with these values).
        tm lastDay = {0};
        lastDay.tm_mday = 31;
        lastDay.tm_mon = 11;
        lastDay.tm_year = date->tm_year - 1;
        // We set time to sometime during the day (midday seems to make sense)
        // so that we don't get problems with daylight saving time.
        lastDay.tm_hour = 12;
        week = getWeek(&lastDay);
    }
    // B) If calculated week is 53, then we need to determine if there really are 53 weeks in current year
    //    or if this is actually week one of the next year.
    else if (week == 53)
    {
        // We need to find out if there really are 53 weeks in this year,
        // There must be 53 weeks in the year if:
        // a) it ends on Thurs (year also starts on Thurs, or Wed on leap year).
        // b) it ends on Friday and starts on Thurs (a leap year).
        // In order not to call mktime again, we can work this out from what we already know!
        int lastDay = date->tm_wday + 31 - date->tm_mday;
        if (lastDay == 5) // Last day of the year is Friday
        {
            // How many days in the year?
            int daysInYear = date->tm_yday + 32 - date->tm_mday; // add 32 because yday is 0 to 365
            if (daysInYear < 366)
            {
                // If 365 days in year, then the year started on Friday
                // so there are only 52 weeks, and this is week one of next year.
                week = 1;
            }
        }
        else if (lastDay != 4) // Last day is NOT Thursday
        {
            // This must be the first week of next year
            week = 1;
        }
        // Otherwise we really have 53 weeks!
    }
    return week;
}

int getWeek(string date)
{
    tm lTM = {0};
    istringstream lTimestamp(date);
    lTimestamp >> get_time(&lTM, "%m/%d/%Y"); // date format : 03/14/2016
    return getWeek(&lTM);
}

int getYear(string date)
{
    return stoi(date.substr(6, 4));
}

int getMonth(string date)
{
    return stoi(date.substr(0, 2));
}

int getDay(string date)
{
    return stoi(date.substr(3, 2));
}

double cpuSecond()
{
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return ((double)tp.tv_sec + (double)tp.tv_usec * 1.e-6);
}