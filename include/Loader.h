#ifndef LOADER_H
#define LOADER_H

#include <string>
#include <vector>
#include <map>

#include "Node.h"

class Loader : public Node
{
public:
    Loader(int myRank, MPI_Comm myCommunicator, std::string path) : Node(myRank, myCommunicator), csv_path(path) {}
    ~Loader() {}

    bool monoReadDataset(std::vector<Row> &data);
    // TODO add stackoverflow credits https://stackoverflow.com/questions/12939279/mpi-reading-from-a-text-file
    bool multiReadDataset(std::vector<Row> &data, int num_workers);

    inline std::map<std::string, int> getCFDict()
    {
        return cfDictionary;
    }

    inline std::map<std::string, int> getBRGHDict()
    {
        return brghDictionary;
    }

private:
    std::string csv_path;

    std::map<std::string, int> cfDictionary;
    std::map<std::string, int> brghDictionary;
};

#endif