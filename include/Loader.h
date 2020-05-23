#ifndef LOADER_H
#define LOADER_H

#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <cstring>
#include <limits.h>

#include "Node.h"

class Loader : public Node
{
public:
    Loader(const std::string &csvFilePath, int myRank = 0, MPI_Comm myCommunicator = 0) : Node(myRank, myCommunicator), csv_path(csvFilePath), numYears(0) {}

    void monoReadDataset(std::vector<Row> &data);
    /* 
    The core structure of this code comes from the Stack Overflow Network (license https://creativecommons.org/licenses/by-sa/4.0/legalcode)
    Link to the original answer/question: https://stackoverflow.com/questions/12939279/mpi-reading-from-a-text-file
    Author: Jonathan Dursi https://stackoverflow.com/users/463827/jonathan-dursi
    */
    void multiReadDataset(std::vector<Row> &data, int num_workers);

    inline Dictionary getCFDict()
    {
        return cfDictionary;
    }

    inline Dictionary getBRGHDict()
    {
        return brghDictionary;
    }

    inline int getNumYears()
    {
        return numYears;
    }

private:
    void pushRow(std::vector<Row> &data, CSVRow row);

    std::string csv_path;

    Dictionary cfDictionary;
    Dictionary brghDictionary;

    int numYears;
};

#endif