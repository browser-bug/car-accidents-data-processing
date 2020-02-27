/* 
The core structure of this code comes from the Stack Overflow Network.
Link to the original answer/question: https://stackoverflow.com/questions/1120140/how-can-i-read-and-parse-csv-files-in-c
Author: Martin York https://stackoverflow.com/users/14065/martin-york
 */

#ifndef CSVROW_H
#define CSVROW_H

#include <iterator>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

#include "utility.h"

using namespace std;

class CSVRow
{
public:
    // int getNumPersonKilled();
    // void print() const;
    // void readNextRow(istream &str);

    int getNumPersonKilled()
    {
        int num_pers_killed = 0;
        if (!m_data[NUMBER_OF_PERSONS_KILLED].empty()) // skip else exceptions are thrown
            num_pers_killed = stoi(m_data[NUMBER_OF_PERSONS_KILLED]);
        return num_pers_killed;
    }

    void print() const
    {
        auto i = m_data.begin();
        if (i != m_data.end())
            cout << *i;

        for (auto it = (++i); it != m_data.end(); ++it)
            cout << ',' << *it;
    }

    void readNextRow(istream &str)
    {
        string line;
        getline(str, line);

        stringstream lineStream(line);
        string cell;

        m_data.clear();
        while (getline(lineStream, cell, ','))
        {
            m_data.push_back(cell);
        }
        // This checks for a trailing comma with no data after it.
        if (!lineStream && cell.empty())
        {
            // If there was a trailing comma then add an empty element.
            m_data.push_back("");
        }
    }

    string const &operator[](size_t index) const
    {
        return m_data[index];
    }

    size_t size() const
    {
        return m_data.size();
    }

private:
    vector<string> m_data; // This contains a general row data
};

istream &operator>>(istream &str, CSVRow &data)
{
    data.readNextRow(str);
    return str;
}

#endif