#ifndef DATA_TYPES_H
#define DATA_TYPES_H

#include <mpi.h>

#include <cstring>

#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53
#define NUM_CONTRIBUTING_FACTORS 47
#define NUM_BOROUGH 5

#define MAX_DATE_LENGTH 11
#define MAX_CF_PER_ROW 5
#define MAX_CF_LENGTH 55
#define MAX_BOROUGH_LENGTH 15

// Data structure representing a row used for pre-processing
typedef struct Row
{
    Row() : num_pers_killed(0), num_contributing_factors(0){};
    Row(int npk,
        int ncf)
        : num_pers_killed(npk), num_contributing_factors(ncf){};

    char date[MAX_DATE_LENGTH] = {};

    int num_pers_killed;

    char contributing_factors[MAX_CF_PER_ROW][MAX_CF_LENGTH] = {};
    int num_contributing_factors;

    char borough[MAX_BOROUGH_LENGTH] = {};

    /* Methods */
    void setDate(std::string newDate)
    {
        strncpy(date, newDate.c_str(), MAX_DATE_LENGTH);
    }
    void setContributingFactor(std::string newContributingFactor)
    {
        strncpy(contributing_factors[num_contributing_factors], newContributingFactor.c_str(), MAX_CF_LENGTH);
        num_contributing_factors++;
    }
    void setBorough(std::string newBorough)
    {
        strncpy(borough, newBorough.c_str(), MAX_BOROUGH_LENGTH);
    }
} Row;

typedef struct AccPair
{
    AccPair() : numAccidents(0), numLethalAccidents(0){};
    AccPair(int na,
            int nla) : numAccidents(na), numLethalAccidents(nla){};

    int numAccidents;
    int numLethalAccidents;
} AccPair;

/* User-defind operation to manage the summation of AccPair during OpenMP reduction */
inline AccPair &operator+=(AccPair &out, const AccPair &in)
{
    out.numAccidents += in.numAccidents;
    out.numLethalAccidents += in.numLethalAccidents;
    return out;
}

/* User-defind operation to manage the summation of AccPair during MPI reduction */
inline void pairSum(void *inputBuffer, void *outputBuffer, int *len, MPI_Datatype *dptr)
{
    AccPair *in = (AccPair *)inputBuffer;
    AccPair *inout = (AccPair *)outputBuffer;

    for (int i = 0; i < *len; ++i)
    {
        inout[i].numAccidents += in[i].numAccidents;
        inout[i].numLethalAccidents += in[i].numLethalAccidents;
    }
}

const int rowLength[] = {
    MAX_DATE_LENGTH,
    1,
    MAX_CF_PER_ROW *MAX_CF_LENGTH,
    1,
    MAX_BOROUGH_LENGTH};
const MPI_Aint rowDisplacements[] = {
    offsetof(Row, date),
    offsetof(Row, num_pers_killed),
    offsetof(Row, contributing_factors),
    offsetof(Row, num_contributing_factors),
    offsetof(Row, borough)};
const MPI_Datatype rowTypes[] = {MPI_CHAR, MPI_INT, MPI_CHAR, MPI_INT, MPI_CHAR};

#endif