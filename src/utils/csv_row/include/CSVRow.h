/* 
The core structure of this code comes from the Stack Overflow Network.
Link to the original answer/question: https://stackoverflow.com/questions/1120140/how-can-i-read-and-parse-csv-files-in-c
Author: Martin York https://stackoverflow.com/users/14065/martin-york
 */

#ifndef CSVROW_H
#define CSVROW_H

#include "../../include/utility.h"

#include <iostream>
#include <algorithm>
#include <vector>
#include <string>
#include <sstream>

class CSVRow
{
public:
    int getNumPersonsKilled();
    std::vector<std::string> getContributingFactors(); /* Returns disting contributing factors in alphabetical order */
    void print() const;
    void readNextRow(std::istream &str);
    void readRowFromString(std::string line);

    inline std::string const &operator[](std::size_t index) const
    {
        return m_data[index];
    }

    inline std::size_t size() const
    {
        return m_data.size();
    }

    inline std::vector<std::string> getData()
    {
        return m_data;
    }

    inline bool isHeader()
    {
        return !(m_data[DATE].compare("DATE"));
    }

private:
    std::vector<std::string> m_data; // This contains a general row data
};

std::istream &operator>>(std::istream &str, CSVRow &data);

#endif
