#ifndef STATS_H
#define STATS_H

#include <mpi.h>
#include <string>
#include <iostream>
#include <fstream>
#include <iterator>

#include "Node.h"

class Stats : public Node
{
public:
    Stats(int myRank, MPI_Comm myCommunicator, int num_workers, int num_omp_threads, const std::string &statsFilePath) : Node(myRank, myCommunicator)
    {
        this->statsFilePath = statsFilePath;
        this->num_workers = num_workers;
        this->num_omp_threads = num_omp_threads;
        avgOverall = 0, avgLoading = 0, avgScattering = 0, avgProcessing = 0, avgWriting = 0;

        if (myRank == 0)
        {
            overallTimes.resize(num_workers);
            loadTimes.resize(num_workers);
            scatterTimes.resize(num_workers);
            procTimes.resize(num_workers);
            writeTimes.resize(num_workers);
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
    ~Stats() {}

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
    int getNumContributingProcesses(std::vector<double> &times);

    std::string statsFilePath;
    std::ofstream outFile;

    int num_workers;
    int num_omp_threads;

    std::vector<double> overallTimes;
    double avgOverall;

    std::vector<double> loadTimes;
    double avgLoading;

    std::vector<double> scatterTimes;
    double avgScattering;

    std::vector<double> procTimes;
    double avgProcessing;

    std::vector<double> writeTimes;
    double avgWriting;
};

#endif