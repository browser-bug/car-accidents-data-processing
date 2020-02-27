#include "CSVRow.h"

using namespace std;

int CSVRow::getNumPersonKilled()
{
    if (m_data[NUMBER_OF_PERSONS_KILLED].empty() || !m_data[NUMBER_OF_PERSONS_KILLED].compare("Unspecified")) // skip else exceptions are thrown
        return 0;
    int num_pers_killed = 0;
    try
    {
        num_pers_killed = stoi(m_data[NUMBER_OF_PERSONS_KILLED]);
    }
    catch (invalid_argument)
    {
        // we just return 0, cases like this are extremely rare anyway
        return 0;
    }
    return num_pers_killed;
}

void CSVRow::print() const
{
    auto i = m_data.begin();
    if (i != m_data.end())
        cout << *i;

    for (auto it = (++i); it != m_data.end(); ++it)
        cout << ',' << *it;
}

void CSVRow::readNextRow(istream &str)
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

istream &operator>>(istream &str, CSVRow &data)
{
    data.readNextRow(str);
    return str;
}