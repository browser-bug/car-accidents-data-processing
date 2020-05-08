#include "Loader.h"

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
The core structure of this code comes from the Stack Overflow Network (license https://creativecommons.org/licenses/by-sa/4.0/legalcode)
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
    char *chunk; // TODO convert this to string?

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
        chunk = (char *)malloc((mysize + 1) * sizeof(char));

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
                pushRow(data, row);
                line.clear();
            }
        }

        free(chunk);

        MPI_File_close(&input_file);
    }
}

void Loader::pushRow(vector<Row> &data, CSVRow row)
{
    if (row.isHeader()) // skip header
        return;

    int num_pers_killed = row.getNumPersonsKilled();
    int num_contributing_factors = 0;
    Row newRow(num_pers_killed, num_contributing_factors);

    string date = row[DATE];
    newRow.setDate(date);

    string borough = row[BOROUGH];
    if (!borough.empty())
    {
        newRow.setBorough(borough);
        // Populating dictionary for QUERY3
        brghDictionary.insert({borough, brghDictionary.size()});
    }

    vector<string> cfs = row.getContributingFactors();
    for (auto cf : cfs)
    {
        newRow.setContributingFactor(cf);
        // Populating dictionary for QUERY2
        cfDictionary.insert({cf, cfDictionary.size()});
    }

    data.push_back(newRow);
}