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
#include <cstddef>

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

void parprocess(MPI_File *in, MPI_File *out, const int rank, const int num_workers, const int overlap)
{
    MPI_Offset globalstart;
    int mysize;
    char *chunk;
    vector<CSVRow> localRows;

    /* read in relevant chunk of file into "chunk",
     * which starts at location in the file globalstart
     * and has size mysize 
     */
    {
        MPI_Offset globalend;
        MPI_Offset filesize;

        /* figure out who reads what */
        MPI_File_get_size(*in, &filesize);
        // cout << "[" << rank << "]"
        //      << "Filesize: " << filesize << endl;
        filesize--; /* get rid of text file eof */
        mysize = filesize / num_workers;
        // cout << "[" << rank << "]"
        //      << "Mysize: " << mysize << endl;
        globalstart = rank * mysize;
        // cout << "[" << rank << "]"
        //      << "Globalstart: " << globalstart << endl;
        globalend = globalstart + mysize - 1;
        // cout << "[" << rank << "]"
        //      << "Globalend: " << globalend << endl;
        if (rank == num_workers - 1) // last proc
            globalend = filesize;

        /* add overlap to the end of everyone's chunk except last proc... */
        if (rank != num_workers - 1)
            globalend += overlap;

        mysize = globalend - globalstart + 1;
        // cout << "[" << rank << "]"
        //      << "Mysize_withoverlap: " << mysize << endl;

        /* allocate memory */
        chunk = (char *)malloc((mysize + 1) * sizeof(char));

        /* everyone reads in their part */
        MPI_File_read_at_all(*in, globalstart, chunk, mysize, MPI_CHAR, MPI_STATUS_IGNORE);
        chunk[mysize] = '\0';
        // cout << "[" << rank << "]"
        //      << "Finished MPI_File_read_at_all\n";

        // Print proc chunk
        // if (rank == 1)
        // {
        //     for (int i = 0; i < mysize; i++)
        //     {
        //         if (chunk[i] != '\n')
        //             cout << chunk[i];
        //         else
        //             cout << endl;
        //     }
        // }
    }

    /*
     * everyone calculate what their start and end *really* are by going
     * from the first newline after start to the first newline after the
     * overlap region starts (eg, after end - overlap + 1)
     */

    int locstart = 0, locend = mysize - 1;
    if (rank != 0) // except the first proc
    {
        while (chunk[locstart] != '\n')
            locstart++;
        locstart++;
    }
    if (rank != num_workers - 1) // except the last proc
    {
        locend -= overlap;
        while (chunk[locend] != '\n')
            locend++;
    }
    mysize = locend - locstart + 1;

    /* printing output for every process */

    CSVRow row;
    string line;
    for (int i = locstart; i <= locend; i++)
    {
        if (chunk[i] != '\n')
        {
            line.push_back(chunk[i]);
        }
        else
        {
            row.readRowFromString(line);
            line.clear();
            localRows.push_back(row);
        }
    }

    /* output the processed file */

    MPI_File_write_at_all(*out, (MPI_Offset)(globalstart + (MPI_Offset)locstart), &(chunk[locstart]), mysize, MPI_CHAR, MPI_STATUS_IGNORE);
    cout << "[" << rank << "]Processed " << mysize << " characters.\n";

    return;
}

int main(int argc, char **argv)
{
    string dataset_path = "../dataset/";
    string in_path = dataset_path + "data_test.csv";
    string out_path = dataset_path + "data_write.csv";

    MPI_File in, out;
    int rank, num_workers;
    int ierr;
    const int overlap = 200;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_workers);

    ierr = MPI_File_open(MPI_COMM_WORLD, in_path.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &in);
    if (ierr)
    {
        if (rank == 0)
            fprintf(stderr, "%s: Couldn't open file %s\n", argv[0], argv[1]);
        MPI_Finalize();
        exit(2);
    }

    ierr = MPI_File_open(MPI_COMM_WORLD, out_path.c_str(), MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &out);
    if (ierr)
    {
        if (rank == 0)
            fprintf(stderr, "%s: Couldn't open output file %s\n", argv[0], argv[2]);
        MPI_Finalize();
        exit(3);
    }

    parprocess(&in, &out, rank, num_workers, overlap);

    MPI_File_close(&in);
    MPI_File_close(&out);

    // Check result
    if (rank == 0)
    {
        ifstream originalFile(in_path);
        ifstream writtenFile(out_path);
        CSVIterator loop1(originalFile);
        CSVIterator loop2(writtenFile);

        while (loop1 != CSVIterator())
        {
            CSVRow original_row = (*loop1);
            CSVRow written_row = (*loop2);

            if (original_row.getData() != written_row.getData())
            {
                cout << "MISMATCH!" << endl;
                original_row.print();
                cout << endl;
                written_row.print();
                break;
            }

            loop1++;
            loop2++;
        }
        cout << "Data writing correctly performed!";
    }

    MPI_Finalize();
}
