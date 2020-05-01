#ifndef NODE_H
#define NODE_H

#include <mpi.h>

#include "../src/utils/include/data_types.h"
#include "../src/utils/csv_row/include/CSVIterator.h"

class Node
{
public:
    Node(int myRank, MPI_Comm myCommunicator) : rank(myRank), comm(myCommunicator) {}

protected:
    int rank;
    MPI_Comm comm;
};

#endif