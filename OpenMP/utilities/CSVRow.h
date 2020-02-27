/* 
The core structure of this code comes from the Stack Overflow Network.
Link to the original answer/question: https://stackoverflow.com/questions/1120140/how-can-i-read-and-parse-csv-files-in-c
Author: Martin York https://stackoverflow.com/users/14065/martin-york
 */

#ifndef CSVROW_H
#define CSVROW_H

#include "utility.h"

#include <iostream>
#include <vector>
#include <string>

class CSVRow
{
public:
    int getNumPersonKilled();
    void print() const;
    void readNextRow(std::istream &str);

    std::string const &operator[](std::size_t index) const
    {
        return m_data[index];
    }

    std::size_t size() const
    {
        return m_data.size();
    }

    // int getNumPersonKilled()
    // {
    //     if (m_data[NUMBER_OF_PERSONS_KILLED].empty() || !m_data[NUMBER_OF_PERSONS_KILLED].compare("Unspecified")) // skip else exceptions are thrown
    //         return 0;
    //     int num_pers_killed = 0;
    //     try
    //     {
    //         num_pers_killed = stoi(m_data[NUMBER_OF_PERSONS_KILLED]);
    //     }
    //     catch (std::invalid_argument)
    //     {
    //         // we just return 0, cases like this are extremely rare anyway
    //         return 0;
    //     }
    //     return num_pers_killed;
    // }

    // void print() const
    // {
    //     auto i = m_data.begin();
    //     if (i != m_data.end())
    //         cout << *i;

    //     for (auto it = (++i); it != m_data.end(); ++it)
    //         cout << ',' << *it;
    // }

    // void readNextRow(istream &str)
    // {
    //     string line;
    //     getline(str, line);

    //     stringstream lineStream(line);
    //     string cell;

    //     m_data.clear();
    //     while (getline(lineStream, cell, ','))
    //     {
    //         m_data.push_back(cell);
    //     }
    //     // This checks for a trailing comma with no data after it.
    //     if (!lineStream && cell.empty())
    //     {
    //         // If there was a trailing comma then add an empty element.
    //         m_data.push_back("");
    //     }
    // }

private:
    std::vector<std::string> m_data; // This contains a general row data
};

std::istream &operator>>(std::istream &str, CSVRow &data)
{
    data.readNextRow(str);
    return str;
}

#endif