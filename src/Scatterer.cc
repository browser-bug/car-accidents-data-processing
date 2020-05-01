#include "../include/Scatterer.h"

#include <cstring>

using namespace std;

void Scatterer::scatterData(vector<Row> *src, vector<Row> *dest, MPI_Datatype type, int csv_size)
{
    int num_rows = 0;
    int *sendcounts;
    int *displs;

    if (rank == 0)
    {
        num_rows = csv_size / num_workers + csv_size % num_workers; // TODO maybe change this so master process doesn't get overloaded with too many rows
        sendcounts = new int[num_workers];
        displs = new int[num_workers];

        sendcounts[0] = num_rows;
        displs[0] = 0;

        int dataOffset = 0;
        for (int i = 1; i < num_workers; i++)
        {
            sendcounts[i] = csv_size / num_workers;
            dataOffset += sendcounts[i - 1];
            displs[i] = dataOffset;
        }
    }

    MPI_Scatter(sendcounts, 1, MPI_INT, &num_rows, 1, MPI_INT, 0, comm);
    dest->resize(num_rows);

    MPI_Scatterv(src->data(), sendcounts, displs, type, dest->data(), num_rows, type, 0, comm);
}

void Scatterer::broadcastDictionary(dictionary &dict, int maxKeyLength)
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
        size_t length;
        for (auto cf : dict)
        {
            length = cf.first.copy(dictKeys[i], cf.first.size());
            dictKeys[i][length] = '\0';
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

    free(dictKeys);
    delete[] dictValues;
}

void Scatterer::mergeDictionary(dictionary &dict, int maxKeyLength)
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
    size_t length;
    for (auto pair : dict)
    {
        length = pair.first.copy(dictKeys[i], pair.first.size());
        dictKeys[i][length] = '\0';
        i++;
    }
    MPI_Allgatherv(dictKeys, totalLength, MPI_CHAR, finalDictKeys, recvcounts, displs, MPI_CHAR, comm);

    // Create new dictionary and distribute it to all processes
    dict.clear();
    for (int i = 0; i < finalNumKeys; i++)
        dict.insert({finalDictKeys[i], dict.size()});

    delete[] recvcounts;
    delete[] displs;
    free(dictKeys);
    free(finalDictKeys);
}
