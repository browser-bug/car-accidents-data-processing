#ifndef PRINTER_H
#define PRINTER_H

#include <string>
#include <iostream>
#include <fstream>

#include "Node.h"

class Printer : public Node
{
public:
    Printer(const std::string &outPath,
            int numYears, int numWeeksPerYear,
            const Dictionary *dictQuery2 = nullptr, const Dictionary *dictQuery3 = nullptr,
            int myRank = 0, MPI_Comm myCommunicator = 0) : Node(myRank, myCommunicator)
    {
        outputFilePath = outPath;

        if (dictQuery2 != nullptr)
            this->dictQuery2 = *dictQuery2;
        if (dictQuery3 != nullptr)
            this->dictQuery3 = *dictQuery3;

        this->numYears = numYears;
        this->numWeeksPerYear = numWeeksPerYear;
    }

    void writeLethAccPerWeek(int *data);     // Query1
    void writeNumAccAndPerc(AccPair *data);  // Query2
    void writeBoroughWeekAcc(AccPair *data); // Query3

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
    int numYears, numWeeksPerYear;

    std::string outputFilePath;
    std::ofstream outFile;

    Dictionary dictQuery2;
    Dictionary dictQuery3;
};

#endif