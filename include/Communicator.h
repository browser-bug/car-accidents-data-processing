#ifndef COMMUNICATOR_H
#define COMMUNICATOR_H

#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <numeric>
#include <functional>

#include "Node.h"

class Communicator : public Node
{
public:
    Communicator(int num_workers, int myRank = 0, MPI_Comm myCommunicator = 0) : Node(myRank, myCommunicator),
                                                                                 num_workers(num_workers) {}

    void scatterData(std::vector<Row> *src, std::vector<Row> *dest, MPI_Datatype type, int csv_size);

    /* Dictionary utilities */
    void sendDictionary(Dictionary &dict, int dest, int tag);
    void recvDictionary(Dictionary &dict, int source, int tag);
    void broadcastDictionary(Dictionary &dict, int root);
    void mergeDictionary(Dictionary &dict, int root);

private:
    int num_workers;

    Dictionary createDictionary(std::vector<std::string> &keys, std::vector<int> &values);
    Keys extractKeys(Dictionary &dict);
    Values extractValues(Dictionary &dict);
};

#endif