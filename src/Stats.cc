#include "Stats.h"

using namespace std;

void Stats::printStats()
{
    cout << fixed << setprecision(8) << endl;
    cout << "********* Timing Statistics *********" << endl;
    cout << "NUM. MPI PROCESSES:\t" << num_workers << endl;
    cout << "NUM. OMP THREADS:\t" << num_omp_threads << endl;

    for (int i = 0; i < num_workers; i++)
    {
        cout << "Process {" << i << "}\t";
        cout << "Loading(" << loadTimes[i] << "), ";
        if (i != 0)
            cout << "Scattering(" << scatterTimes[i] << "), ";
        cout << "Processing(" << procTimes[i] << "), ";
        cout << "Writing(" << writeTimes[i] << "), ";
        cout << "took overall " << overallTimes[i] << " seconds" << endl;
    }

    cout << endl
         << "With average times of: "
         << "[1] Loading(" << avgLoading << "), "
         << "[1a] Scattering(" << avgScattering << "), "
         << "[2] Processing(" << avgProcessing << "), "
         << "[3] Writing(" << avgWriting << "),\t"
         << "Overall(" << avgOverall << ").\n";
}

void Stats::writeStats()
{
    outFile << avgLoading << ","
            << avgScattering << ","
            << avgProcessing << ","
            << avgWriting << ","
            << avgOverall
            << endl;
}

void Stats::computeAverages()
{
    for (int i = 0; i < num_workers; i++)
    {
        avgOverall += overallTimes[i];
        avgLoading += loadTimes[i];
        if (i != 0)
            avgScattering += scatterTimes[i];
        avgProcessing += procTimes[i];
        avgWriting += writeTimes[i];
    }

    avgOverall /= getNumContributingProcesses(overallTimes);
    avgLoading /= getNumContributingProcesses(loadTimes);
    avgScattering /= getNumContributingProcesses(scatterTimes);
    avgProcessing /= getNumContributingProcesses(procTimes);
    avgWriting /= getNumContributingProcesses(writeTimes);
}

int Stats::getNumContributingProcesses(vector<double> &times)
{
    return count_if(times.begin(), times.end(), [](double i) { return i > 0; });
}

bool Stats::setOverallTimes(double *duration)
{
    if (num_workers == 1)
        overallTimes[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, overallTimes.data(), 1, MPI_DOUBLE, 0, comm);
}
bool Stats::setLoadTimes(double *duration)
{
    if (num_workers == 1)
        loadTimes[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, loadTimes.data(), 1, MPI_DOUBLE, 0, comm);
}
bool Stats::setScatterTimes(double *duration)
{
    if (num_workers == 1)
        scatterTimes[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, scatterTimes.data(), 1, MPI_DOUBLE, 0, comm);
}
bool Stats::setProcTimes(double *duration)
{
    if (num_workers == 1)
        procTimes[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, procTimes.data(), 1, MPI_DOUBLE, 0, comm);
}
bool Stats::setWriteTimes(double *duration)
{
    if (num_workers == 1)
        writeTimes[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, writeTimes.data(), 1, MPI_DOUBLE, 0, comm);
}