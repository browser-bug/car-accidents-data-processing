#ifndef LOADER_H
#define LOADER_H

#include <string>
#include <vector>
#include <map>

#include "Node.h"

using dictionary = std::map<std::string, int>;

class Loader : public Node
{
public:
    Loader(int myRank, MPI_Comm myCommunicator, std::string path) : Node(myRank, myCommunicator), csv_path(path) {}
    ~Loader() {}

    void monoReadDataset(std::vector<Row> &data);
    // TODO add stackoverflow credits https://stackoverflow.com/questions/12939279/mpi-reading-from-a-text-file
    void multiReadDataset(std::vector<Row> &data, int num_workers);

    inline dictionary getCFDict()
    {
        return cfDictionary;
    }

    inline dictionary getBRGHDict()
    {
        return brghDictionary;
    }

private:
    std::string csv_path;

    dictionary cfDictionary;
    dictionary brghDictionary;
};

#endif