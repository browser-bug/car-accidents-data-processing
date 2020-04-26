#include "../include/Scatterer.h"

#include <cstring>

using namespace std;

bool Scatterer::scatterData(vector<Row> *src, vector<Row> *dest, MPI_Datatype type, int csv_size)
{
    if (rank == 0)
    {
        scatterCount[rank] = csv_size / num_workers + csv_size % num_workers; // TODO maybe change this so master process doesn't get overloaded with too many rows
        num_rows = scatterCount[rank];

        dataDispl[rank] = 0;

        int datasetOffset = 0;
        for (int rank = 1; rank < num_workers; rank++)
        {
            scatterCount[rank] = csv_size / num_workers;
            datasetOffset += scatterCount[rank - 1];
            dataDispl[rank] = datasetOffset;

            MPI_Send(&scatterCount[rank], 1, MPI_INT, rank, 13, comm);
        }
    }
    else
    {
        MPI_Recv(&num_rows, 1, MPI_INT, 0, 13, comm, MPI_STATUS_IGNORE);
    }

    // Scattering data to all workers
    dest->resize(num_rows);
    MPI_Scatterv(src->data(), scatterCount, dataDispl, type, dest->data(), num_rows, type, 0, comm);
}

bool Scatterer::broadcastDictionary(dictionary &dict, int maxKeyLength)
{
    int numKeys = dict.size();
    MPI_Bcast(&numKeys, 1, MPI_INT, 0, comm);

    char(*dictKeys)[maxKeyLength];
    int *dictValues;
    dictKeys = (char(*)[maxKeyLength])malloc(numKeys * sizeof(*dictKeys));
    dictValues = new int[numKeys];

    if (rank == 0)
    {
        int i = 0;
        for (auto cf : dict)
        {
            strncpy(dictKeys[i], cf.first.c_str(), maxKeyLength);
            dictValues[i] = cf.second;
            i++;
        }
    }

    MPI_Bcast(dictKeys, numKeys * maxKeyLength, MPI_CHAR, 0, comm);
    MPI_Bcast(dictValues, numKeys, MPI_INT, 0, comm);

    if (rank != 0)
    {
        for (int i = 0; i < numKeys; i++)
            dict.insert({dictKeys[i], dictValues[i]});
    }

    delete[] dictKeys;
    delete[] dictValues;
}

bool Scatterer::mergeDictionary(dictionary &dict, int maxKeyLength)
{
    // Calculate destination dictionary size
    int numKeys = dict.size();
    int totalLength = numKeys * maxKeyLength;
    int finalNumKeys = 0;
    MPI_Allreduce(&numKeys, &finalNumKeys, 1, MPI_INT, MPI_SUM, comm);

    // Computing number of elements that are received from each process
    int *recvcounts = new int[num_workers];
    MPI_Allgather(&totalLength, 1, MPI_INT, recvcounts, 1, MPI_INT, comm);

    // Computing displacement relative to recvbuf at which to place the incoming data from each process
    int *displs = new int[num_workers];
    if (rank == 0)
    {
        displs[0] = 0;
        for (int i = 1; i < num_workers; i++)
            displs[i] = displs[i - 1] + recvcounts[i - 1];
    }
    MPI_Bcast(displs, num_workers, MPI_INT, 0, comm);

    char(*dictKeys)[maxKeyLength];
    char(*finalDictKeys)[maxKeyLength];
    dictKeys = (char(*)[maxKeyLength])malloc(numKeys * sizeof(*dictKeys));
    finalDictKeys = (char(*)[maxKeyLength])malloc(finalNumKeys * sizeof(*finalDictKeys));

    // Collect keys for each process
    int i = 0;
    for (auto pair : dict)
    {
        strncpy(dictKeys[i], pair.first.c_str(), maxKeyLength);
        i++;
    }
    MPI_Allgatherv(dictKeys, totalLength, MPI_CHAR, finalDictKeys, recvcounts, displs, MPI_CHAR, comm);

    // Create new dictionary and distribute it to all processes
    dict.clear();
    for (int i = 0; i < finalNumKeys; i++)
        dict.insert({finalDictKeys[i], dict.size()});

    delete[] recvcounts;
    delete[] displs;
    delete[] dictKeys;
    delete[] finalDictKeys;
}
