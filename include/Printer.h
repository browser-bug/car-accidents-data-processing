#ifndef PRINTER_H
#define PRINTER_H

#include <string>
#include <iostream>
#include <fstream>

#include "Node.h"

using dictionary = std::map<std::string, int>;

class Printer : public Node
{
public:
    Printer(const std::string &outPath,
            const dictionary *dictQuery2 = nullptr, const dictionary *dictQuery3 = nullptr,
            int myRank = 0, MPI_Comm myCommunicator = 0) : Node(myRank, myCommunicator)
    {
        outputFilePath = outPath;

        if (dictQuery2 != nullptr)
            this->dictQuery2 = *dictQuery2;
        if (dictQuery3 != nullptr)
            this->dictQuery3 = *dictQuery3;
    }

    void writeQuery1(int data[][NUM_WEEKS_PER_YEAR]);
    void writeQuery2(AccPair data[NUM_BOROUGH]);
    void writeQuery3(AccPair data[][NUM_YEARS][NUM_WEEKS_PER_YEAR]);

    inline bool openFile()
    {
        outFile = std::ofstream(outputFilePath);
        if (outFile.fail())
            return false;
        outFile.clear();
        return true;
    }
    inline void closeFile()
    {
        outFile.close();
    }

private:
    std::string outputFilePath;
    std::ofstream outFile;

    dictionary dictQuery2;
    dictionary dictQuery3;
};

#endif