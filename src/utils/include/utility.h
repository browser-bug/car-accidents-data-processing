#ifndef UTILITY_H
#define UTILITY_H

#include <iomanip>
#include <iostream>
#include <string>
#include <sstream>
#include <map>
#include <sys/time.h>

/* days per month -- nonleap! */
const short __spm[13] =
    {
        0,
        (31),
        (31 + 28),
        (31 + 28 + 31),
        (31 + 28 + 31 + 30),
        (31 + 28 + 31 + 30 + 31),
        (31 + 28 + 31 + 30 + 31 + 30),
        (31 + 28 + 31 + 30 + 31 + 30 + 31),
        (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31),
        (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30),
        (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31),
        (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30),
        (31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31),
};

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

/* Boroughs Dictionary */
const std::map<std::string, int> static_brghDictionary =
    {
        {"MANHATTAN", 0},
        {"QUEENS", 1},
        {"BRONX", 2},
        {"BROOKLYN", 3},
        {"STATEN ISLAND", 4}};

/* Contributing Factors Dictionary */
const std::map<std::string, int> static_cfDictionary =
    {
        {"Accelerator Defective", 33},
        {"Aggressive Driving/Road Rage", 27},
        {"Alcohol Involvement", 19},
        {"Animals Action", 30},
        {"Backing Unsafely", 4},
        {"Brakes Defective", 20},
        {"Cell Phone (hand-held)", 37},
        {"Cell Phone (hands-free)", 40},
        {"Driver Inattention/Distraction", 0},
        {"Driver Inexperience", 6},
        {"Driverless/Runaway Vehicle", 46},
        {"Drugs (Illegal)", 38},
        {"Failure to Keep Right", 24},
        {"Failure to Yield Right-of-Way", 13},
        {"Fatigued/Drowsy", 5},
        {"Fell Asleep", 28},
        {"Following Too Closely", 35},
        {"Glare", 26},
        {"Headlights Defective", 42},
        {"Illness", 9},
        {"Lane Marking Improper/Inadequate", 22},
        {"Lost Consciousness", 1},
        {"Obstruction/Debris", 8},
        {"Other Electronic Device", 25},
        {"Other Lighting Defects", 43},
        {"Other Vehicular", 7},
        {"Outside Car Distraction", 11},
        {"Oversized Vehicle", 18},
        {"Passenger Distraction", 14},
        {"Passing or Lane Usage Improper", 36},
        {"Pavement Defective", 15},
        {"Pavement Slippery", 2},
        {"Pedestrian/Bicyclist/Other Pedestrian Error/Confusion", 32},
        {"Physical Disability", 21},
        {"Prescription Medication", 3},
        {"Reaction to Other Uninvolved Vehicle", 12},
        {"Shoulders Defective/Improper", 41},
        {"Steering Failure", 17},
        {"Tire Failure/Inadequate", 29},
        {"Tow Hitch Defective", 44},
        {"Traffic Control Device Improper/Non-Working", 34},
        {"Traffic Control Disregarded", 16},
        {"Turning Improperly", 10},
        {"Unsafe Lane Changing", 39},
        {"Unsafe Speed", 31},
        {"View Obstructed/Limited", 23},
        {"Windshield Inadequate", 45}};

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

/* This creates a dictonary from an array of UNIQUE words. */
std::map<std::string, int> createDictionary(int num_keys, char **keys, int *values);

double cpuSecond();

#endif
