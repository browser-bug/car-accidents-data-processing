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
        cout << "Loading(" << load.times[i] << "), ";
        if (i != 0)
            cout << "Scattering(" << scatter.times[i] << "), ";
        cout << "Processing(" << proc.times[i] << "), ";
        cout << "Writing(" << write.times[i] << "), ";
        cout << "took overall " << overall.times[i] << " seconds" << endl;
    }

    cout << endl
         << "With average times of: "
         << "[1] Loading(" << load.average << "), "
         << "[1a] Scattering(" << scatter.average << "), "
         << "[2] Processing(" << proc.average << "), "
         << "[3] Writing(" << write.average << "),\t"
         << "Overall(" << overall.average << ").\n";
}

void Stats::writeStats()
{
    outFile << load.average << ","
            << scatter.average << ","
            << proc.average << ","
            << write.average << ","
            << overall.average
            << endl;
}

void Stats::computeAverages()
{
    for (int i = 0; i < num_workers; i++)
    {
        overall.average += overall.times[i];
        load.average += load.times[i];
        if (i != 0)
            scatter.average += scatter.times[i];
        proc.average += proc.times[i];
        write.average += write.times[i];
    }

    overall.average /= overall.getNumContributingProcesses();
    load.average /= load.getNumContributingProcesses();
    scatter.average /= scatter.getNumContributingProcesses();
    proc.average /= proc.getNumContributingProcesses();
    write.average /= write.getNumContributingProcesses();
}

void Stats::setOverallTimes(double *duration)
{
    if (num_workers == 1)
        overall.times[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, overall.times.data(), 1, MPI_DOUBLE, 0, comm);
}
void Stats::setLoadTimes(double *duration)
{
    if (num_workers == 1)
        load.times[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, load.times.data(), 1, MPI_DOUBLE, 0, comm);
}
void Stats::setScatterTimes(double *duration)
{
    if (num_workers == 1)
        scatter.times[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, scatter.times.data(), 1, MPI_DOUBLE, 0, comm);
}
void Stats::setProcTimes(double *duration)
{
    if (num_workers == 1)
        proc.times[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, proc.times.data(), 1, MPI_DOUBLE, 0, comm);
}
void Stats::setWriteTimes(double *duration)
{
    if (num_workers == 1)
        write.times[0] = *duration;
    else
        MPI_Gather(duration, 1, MPI_DOUBLE, write.times.data(), 1, MPI_DOUBLE, 0, comm);
}