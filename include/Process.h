#ifndef PROCESS_H
#define PROCESS_H

#include <omp.h>
#include <map>
#include <string>

#include "Node.h"

using dictionary = std::map<std::string, int>;

class Process : public Node
{
public:
    Process(int myRank, MPI_Comm myCommunicator, const dictionary *dictQuery2 = nullptr, const dictionary *dictQuery3 = nullptr) : Node(myRank, myCommunicator)
    {
        if (dictQuery2 != nullptr)
            this->dictQuery2 = *dictQuery2;
        if (dictQuery3 != nullptr)
            this->dictQuery3 = *dictQuery3;
    }
    ~Process() {}

    void processQuery1(const Row &data, int result[][NUM_WEEKS_PER_YEAR]);
    void processQuery2(const Row &data, AccPair result[NUM_BOROUGH]);
    void processQuery3(const Row &data, AccPair result[][NUM_YEARS][NUM_WEEKS_PER_YEAR]);

private:
    void computeWeekAndYear(std::string date, int &week, int &year);

    dictionary dictQuery2;
    dictionary dictQuery3;
};

#endif