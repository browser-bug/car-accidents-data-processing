#ifndef UTILITY_H
#define UTILITY_H

#include <iomanip>
#include <string>
#include <sys/time.h>

// Utility enumeration for indexing the csv file
enum CSV_INDEXES
{
    DATE,
    TIME,
    BOROUGH,
    ZIP_CODE,
    LATITUDE,
    LONGITUDE,
    LOCATION,
    ON_STREET_NAME,
    CROSS_STREET_NAME,
    OFF_STREET_NAME,
    NUMBER_OF_PERSONS_INJURED,
    NUMBER_OF_PERSONS_KILLED,
    NUMBER_OF_PEDESTRIANS_INJURED,
    NUMBER_OF_PEDESTRIANS_KILLED,
    NUMBER_OF_CYCLIST_INJURED,
    NUMBER_OF_CYCLIST_KILLED,
    NUMBER_OF_MOTORIST_INJURED,
    NUMBER_OF_MOTORIST_KILLED,
    CONTRIBUTING_FACTOR_VEHICLE_1,
    CONTRIBUTING_FACTOR_VEHICLE_2,
    CONTRIBUTING_FACTOR_VEHICLE_3,
    CONTRIBUTING_FACTOR_VEHICLE_4,
    CONTRIBUTING_FACTOR_VEHICLE_5,
    UNIQUE_KEY,
    VEHICLE_TYPE_CODE_1,
    VEHICLE_TYPE_CODE_2,
    VEHICLE_TYPE_CODE_3,
    VEHICLE_TYPE_CODE_4,
    VEHICLE_TYPE_CODE_5
};

/* 
The core structure of the getWeek function code comes from the Codeproject Network.
Link to the original answer/question: https://www.codeproject.com/questions/592534/helpplustoplusfindoutplusweekplusnumberplusofplusy
Author: Ian A Davidson https://www.codeproject.com/script/Membership/View.aspx?mid=1743676
 */
int getWeek(struct std::tm *date);
int getWeek(std::string date);

int getYear(std::string date);
int getMonth(std::string date);
int getDay(std::string date);

double cpuSecond();

#endif