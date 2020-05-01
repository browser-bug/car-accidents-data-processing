#ifndef PROCESS_H
#define PROCESS_H

#include <omp.h>
#include <map>
#include <string>

#include "Node.h"

using dictionary = std::map<std::string, int>;

struct week_and_year
{
    int week;
    int year;
};

class Process : public Node
{
public:
    Process(const dictionary *dictQuery2 = nullptr, const dictionary *dictQuery3 = nullptr, int myRank = 0, MPI_Comm myCommunicator = 0) : Node(myRank, myCommunicator)
    {
        if (dictQuery2 != nullptr)
            this->dictQuery2 = *dictQuery2;
        if (dictQuery3 != nullptr)
            this->dictQuery3 = *dictQuery3;
    }

    void processQuery1(const Row &data, int result[][NUM_WEEKS_PER_YEAR]);
    void processQuery2(const Row &data, AccPair result[NUM_BOROUGH]);
    void processQuery3(const Row &data, AccPair result[][NUM_YEARS][NUM_WEEKS_PER_YEAR]);

private:
    week_and_year computeWeekAndYear(std::string date);

    dictionary dictQuery2;
    dictionary dictQuery3;
};

#endif