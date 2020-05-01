#include "Loader.h"

#include <iostream>
#include <fstream>
#include <cstring>

using namespace std;

void Loader::monoReadDataset(vector<Row> &data)
{
    cout << "[Proc. " + to_string(rank) + "] Started loading dataset..." << endl;
    ifstream file(csv_path);

    CSVRow row;
    for (CSVIterator loop(file); loop != CSVIterator(); ++loop)
    {
        row = (*loop);
        pushRow(data, row);
    }
}

/* 
The core structure of this code comes from the Stack Overflow Network.
Link to the original answer/question: https://stackoverflow.com/questions/12939279/mpi-reading-from-a-text-file
Author: Jonathan Dursi https://stackoverflow.com/users/463827/jonathan-dursi
*/
void Loader::multiReadDataset(vector<Row> &data, int num_workers)
{
    cout << "[Proc. " + to_string(rank) + "] Started loading dataset..." << endl;
    MPI_File input_file;
    const int overlap = 200; // used to define overlapping intervals during csv reading phase

    MPI_File_open(comm, csv_path.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &input_file);

    MPI_Offset globalstart;
    int mysize;
    char *chunk;

    /* read in relevant chunk of file into "chunk",
     * which starts at location in the file globalstart
     * and has size mysize 
     */
    {
        MPI_Offset globalend;
        MPI_Offset filesize;

        /* figure out who reads what */
        MPI_File_get_size(input_file, &filesize);
        filesize--; /* get rid of text file eof */
        mysize = filesize / num_workers;

        globalstart = rank * mysize;

        /* last proc gets the biggest slice in case mysize division has a reminder */
        if (rank == num_workers - 1)
            globalend = filesize;

        /* add overlap to the end of everyone's chunk except last proc... */
        if (rank != num_workers - 1)
            globalend = (globalstart + mysize - 1) + overlap;

        mysize = globalend - globalstart + 1; // fix the size of every proc

        /* allocate memory */
        chunk = new char[mysize + 1];

        /* everyone reads in their part */
        MPI_File_read_at_all(input_file, globalstart, chunk, mysize, MPI_CHAR, MPI_STATUS_IGNORE);
        chunk[mysize] = '\0';

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
                pushRow(data, row);
            }
        }

        delete[] chunk;
        MPI_File_close(&input_file);
    }
}

void Loader::pushRow(vector<Row> &data, CSVRow row)
{
    if (!row[TIME].compare("TIME")) // TODO: find a nicer way to skip the header
        return;

    string date = row[DATE];
    string borough = row[BOROUGH];
    vector<string> cfs = row.getContributingFactors();

    Row newRow(row.getNumPersonsKilled(), 0);

    strncpy(newRow.date, date.c_str(), DATE_LENGTH);

    for (unsigned int k = 0; k < cfs.size(); k++)
    {
        strncpy(newRow.contributing_factors[k], cfs[k].c_str(), MAX_CF_LENGTH);
        newRow.num_contributing_factors++;

        // Populating dictionary for QUERY2
        cfDictionary.insert({cfs[k], cfDictionary.size()});
    }

    if (!borough.empty())
    {
        strncpy(newRow.borough, borough.c_str(), MAX_BOROUGH_LENGTH);

        // Populating dictionary for QUERY3
        brghDictionary.insert({borough, brghDictionary.size()});
    }

    data.push_back(newRow);
}