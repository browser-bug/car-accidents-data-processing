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
    Loader(const std::string &csvFilePath, int myRank = 0, MPI_Comm myCommunicator = 0) : Node(myRank, myCommunicator), csv_path(csvFilePath) {}

    void monoReadDataset(std::vector<Row> &data);
    /* 
    The core structure of this code comes from the Stack Overflow Network (license https://creativecommons.org/licenses/by-sa/4.0/legalcode)
    Link to the original answer/question: https://stackoverflow.com/questions/12939279/mpi-reading-from-a-text-file
    Author: Jonathan Dursi https://stackoverflow.com/users/463827/jonathan-dursi
    */
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
    void pushRow(std::vector<Row> &data, CSVRow row);

    std::string csv_path;

    dictionary cfDictionary;
    dictionary brghDictionary;
};

#endif