#ifndef STATS_H
#define STATS_H

#include <mpi.h>
#include <string>
#include <iostream>
#include <fstream>
#include <iterator>

#include "Node.h"

struct TimeStats
{
    TimeStats() : average(0) {}

    std::vector<double> times;
    double average;

    int getNumContributingProcesses()
    {
        return count_if(times.begin(), times.end(), [](double i) { return i > 0; });
    }
};

class Stats : public Node
{
public:
    Stats(const std::string &statsFilePath, int myRank = 0, MPI_Comm myCommunicator = 0, int num_workers = 1, int num_omp_threads = 1) : Node(myRank, myCommunicator)
    {
        this->statsFilePath = statsFilePath;
        this->num_workers = num_workers;
        this->num_omp_threads = num_omp_threads;

        if (myRank == 0)
        {
            overall.times.resize(num_workers);
            load.times.resize(num_workers);
            scatter.times.resize(num_workers);
            proc.times.resize(num_workers);
            write.times.resize(num_workers);
        }

        std::ifstream checkFile(statsFilePath);
        if (!checkFile.good())
        {
            // if csv doesn't exists create file and add header first
            outFile = std::ofstream(statsFilePath);
            outFile << "Loading,Scattering,Processing,Writing,Overall" << std::endl;
            outFile.close();
        }
    }

    void printStats();
    void writeStats();

    void computeAverages();

    void setOverallTimes(double *duration);
    void setLoadTimes(double *duration);
    void setScatterTimes(double *duration);
    void setProcTimes(double *duration);
    void setWriteTimes(double *duration);

    inline bool openFile()
    {
        outFile = std::ofstream(statsFilePath, std::ios::app);
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
    std::string statsFilePath;
    std::ofstream outFile;

    int num_workers;
    int num_omp_threads;

    TimeStats overall;
    TimeStats load;
    TimeStats scatter;
    TimeStats proc;
    TimeStats write;
};

#endif