#ifndef PROCESS_H
#define PROCESS_H

#include <omp.h>
#include <map>
#include <string>

#include "Node.h"

struct week_and_year
{
    int week;
    int year;
};

class Process : public Node
{
public:
    Process(int numYears, int numWeeksPerYear, const Dictionary *dictQuery2 = nullptr, const Dictionary *dictQuery3 = nullptr, int myRank = 0, MPI_Comm myCommunicator = 0) : Node(myRank, myCommunicator)
    {
        if (dictQuery2 != nullptr)
            this->dictQuery2 = *dictQuery2;
        if (dictQuery3 != nullptr)
            this->dictQuery3 = *dictQuery3;

        this->numYears = numYears;
        this->numWeeksPerYear = numWeeksPerYear;
    }

    void processLethAccPerWeek(Row &data, int *result);
    void processNumAccAndPerc(Row &data, AccPair *result);
    void processBoroughWeekAcc(Row &data, AccPair *result);

private:
    int numYears, numWeeksPerYear;
    week_and_year computeWeekAndYear(std::string date);

    Dictionary dictQuery2;
    Dictionary dictQuery3;
};

#endif