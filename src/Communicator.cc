#include "Communicator.h"

using namespace std;

void Communicator::scatterData(std::vector<Row> *sendbuf, int buf_size, MPI_Datatype sendtype, std::vector<Row> *recvbuf, MPI_Datatype recvtype, int root)
{
    int num_rows = 0;
    int *sendcounts;
    int *displs;

    if (rank == 0)
    {
        num_rows = buf_size / num_workers + buf_size % num_workers; // TODO maybe change this so master process doesn't get overloaded with too many rows
        sendcounts = new int[num_workers];
        displs = new int[num_workers];

        sendcounts[0] = num_rows;
        displs[0] = 0;

        int dataOffset = 0;
        for (int i = 1; i < num_workers; i++)
        {
            sendcounts[i] = buf_size / num_workers;
            dataOffset += sendcounts[i - 1];
            displs[i] = dataOffset;
        }
    }

    MPI_Scatter(sendcounts, 1, MPI_INT, &num_rows, 1, MPI_INT, 0, comm);
    recvbuf->resize(num_rows);

    MPI_Scatterv(sendbuf->data(), sendcounts, displs, sendtype, recvbuf->data(), num_rows, recvtype, root, comm);
}

void Communicator::sendDictionary(Dictionary &dict, int dest, int tag)
{
    int numKeys = dict.size();
    MPI_Send(&numKeys, 1, MPI_INT, dest, tag, comm);

    /* Extract dictionary info */
    Keys dictKeys = extractKeys(dict);
    Values dictValues = extractValues(dict);

    /* Send every key size */
    int key_size[numKeys];
    for (int i = 0; i < numKeys; i++)
        key_size[i] = dictKeys[i].size();
    MPI_Send(key_size, numKeys, MPI_INT, dest, tag, comm);

    /* Send every key */
    for (int i = 0; i < numKeys; i++)
        MPI_Send(&(dictKeys[i][0]), key_size[i], MPI_CHAR, dest, tag + i, comm);

    /* Send every value */
    MPI_Send(dictValues.data(), numKeys, MPI_INT, dest, tag, comm);
}

void Communicator::recvDictionary(Dictionary &dict, int source, int tag)
{
    int numKeys;
    MPI_Recv(&numKeys, 1, MPI_INT, source, tag, comm, MPI_STATUS_IGNORE);

    Keys dictKeys(numKeys);
    Values dictValues(numKeys);

    /* Receive every key size */
    int key_size[numKeys];
    MPI_Recv(key_size, numKeys, MPI_INT, source, tag, comm, MPI_STATUS_IGNORE);

    /* Resize every key with correct size */
    for (int i = 0; i < numKeys; i++)
        dictKeys[i].resize(key_size[i]);

    /* Receive every key */
    for (int i = 0; i < numKeys; i++)
        MPI_Recv(&(dictKeys[i][0]), key_size[i], MPI_CHAR, source, tag + i, comm, MPI_STATUS_IGNORE);

    /* Receive every value */
    MPI_Recv(dictValues.data(), numKeys, MPI_INT, source, tag, comm, MPI_STATUS_IGNORE);

    /* Create a new dictionary */
    dict.clear();
    dict = createDictionary(dictKeys, dictValues);
}

void Communicator::broadcastDictionary(Dictionary &dict, int root)
{
    if (rank == root)
    {
        for (int dest = 0; dest < num_workers; dest++)
        {
            if (dest != root)
                sendDictionary(dict, dest, dest);
        }
    }
    else
    {
        recvDictionary(dict, root, rank);
    }
}

void Communicator::mergeDictionary(Dictionary &dict, int root)
{
    Dictionary recv_dict[num_workers]; // Collect every process dictionary inside here

    /* root will receive all the dictionaries */
    if (rank == root)
    {
        recv_dict[root] = dict;
        for (int prank = 0; prank < num_workers; prank++)
        {
            if (prank != root)
            {
                recvDictionary(recv_dict[prank], prank, prank);
            }
        }
    }
    else
    {
        sendDictionary(dict, root, rank);
    }

    MPI_Barrier(comm);

    /* Merge all the dictionaries removing duplicates */
    if (rank == root)
    {
        dict.clear();
        for (int i = 0; i < num_workers; i++)
        {
            for (auto pair : recv_dict[i])
            {
                dict.insert({pair.first, dict.size()});
            }
        }
    }

    /* Broadcast the merged dictionary */
    broadcastDictionary(dict, root);
}

Dictionary Communicator::createDictionary(Keys &keys, Values &values)
{
    Dictionary dict;
    for (unsigned int i = 0; i < keys.size(); i++)
        dict.insert({keys[i], values[i]});
    return dict;
}

Keys Communicator::extractKeys(Dictionary &dict)
{
    Keys k;
    for (auto pair : dict)
        k.push_back(pair.first);
    return k;
}

Values Communicator::extractValues(Dictionary &dict)
{
    Values v;
    for (auto pair : dict)
        v.push_back(pair.second);
    return v;
}