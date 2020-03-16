#include <mpi.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <string>
#include <string.h> // for string copy

#include "../utilities/CSVIterator.h"

#define MAX_CF_LENGTH 55

using namespace std;

typedef struct AccPair
{
    AccPair() : numAccidents(0), numPersKilled(0){};
    AccPair(int na,
            int nla) : numAccidents(na), numPersKilled(nla){};

    int numAccidents;
    int numPersKilled;
} AccPair;

void pairSum(void *inputBuffer, void *outputBuffer, int *len, MPI_Datatype *dptr)
{
    AccPair *in = (AccPair *)inputBuffer;
    AccPair *inout = (AccPair *)outputBuffer;

    for (int i = 0; i < *len; ++i)
    {
        inout[i].numAccidents += in[i].numAccidents;
        inout[i].numPersKilled += in[i].numPersKilled;
    }
}

int main(int argc, char **argv)
{

    AccPair global_accAndPerc[4];
    map<string, int> dictonary;

    MPI_Datatype accPairType;
    // TODO add these two lines if everything works out
    // MPI_Type_contiguous( 2, MPI_INT, &accPairType );
    // MPI_Type_commit( &accPairType );
    int aptLength[2] = {1, 1};
    MPI_Aint aptDisplacements[2] = {
        offsetof(AccPair, numAccidents),
        offsetof(AccPair, numPersKilled)};
    MPI_Datatype aptTypes[2] = {MPI_INT, MPI_INT};

    // MPI Operators definitions
    MPI_Op accPairSum;

    int myrank, num_workers;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);

    MPI_Type_create_struct(2, aptLength, aptDisplacements, aptTypes, &accPairType);
    MPI_Type_commit(&accPairType);

    MPI_Op_create(pairSum, true, &accPairSum);

    srand(time(0));
    AccPair local_accAndPerc[4] = {AccPair(rand() % 10, rand() % 10), AccPair(rand() % 10, rand() % 10), AccPair(rand() % 10, rand() % 10), AccPair(rand() % 10, rand() % 10)};

    if (myrank == 0)
    {

        char borough[15] = {};
        string s = "MANHATTAN";
        strncpy(borough, s.c_str(), 15);

        for (int i = 0; i < 15; i++)
            cout << borough[i] << endl;

        cout << boroughDictionary.at(borough);
    }
    MPI_Finalize();
}