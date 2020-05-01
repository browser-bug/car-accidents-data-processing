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
    Scatterer(int num_workers, int myRank = 0, MPI_Comm myCommunicator = 0) : Node(myRank, myCommunicator),
                                                                              num_workers(num_workers) {}

    void scatterData(std::vector<Row> *src, std::vector<Row> *dest, MPI_Datatype type, int csv_size);
    void broadcastDictionary(dictionary &dict, int maxKeyLength);
    void mergeDictionary(dictionary &dict, int maxKeyLength);

private:
    int num_workers;
};

#endif