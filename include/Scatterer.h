#ifndef SCATTERER_H
#define SCATTERER_H

#include <map>
#include <vector>
#include <string>

#include "Node.h"

using dictionary = std::map<std::string, int>;

class Scatterer : public Node
{
public:
    Scatterer(int myRank, MPI_Comm myCommunicator, int num_workers) : Node(myRank, myCommunicator)
    {
        this->num_workers = num_workers;
        if (myRank == 0)
        {
            scatterCount = new int[num_workers];
            dataDispl = new int[num_workers];
        }
    }
    ~Scatterer()
    {
        if (rank == 0)
        {
            delete[] scatterCount;
            delete[] dataDispl;
        }
    }

    bool scatterData(std::vector<Row> *src, std::vector<Row> *dest, MPI_Datatype type, int csv_size);
    bool broadcastDictionary(dictionary &dict, int maxKeyLength);
    bool mergeDictionary(dictionary &dict, int maxKeyLength);

    inline int getNumRows()
    {
        return num_rows;
    }

private:
    int num_workers;

    int *scatterCount;
    int *dataDispl;
    int num_rows;
};

#endif