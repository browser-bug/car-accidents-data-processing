#include "../include/CSVRow.h"

using namespace std;

int CSVRow::getNumPersonsKilled()
{
    int num_ped_killed, num_cycl_killed, num_mot_killed;
    try
    {
        num_ped_killed = stoi(m_data[NUMBER_OF_PEDESTRIANS_KILLED]);
        num_cycl_killed = stoi(m_data[NUMBER_OF_CYCLIST_KILLED]);
        num_mot_killed = stoi(m_data[NUMBER_OF_MOTORIST_KILLED]);

        return (num_ped_killed + num_cycl_killed + num_mot_killed);
    }
    catch (invalid_argument)
    {
        // we just return 0, cases like this are extremely rare anyway
        return 0;
    }
}

// Filtering contributing factors
bool filter(string cf)
{
    return cf.empty() || !cf.compare("Unspecified");
}

vector<string> CSVRow::getContributingFactors()
{
    // Extracting the contributing factors
    auto first = m_data.begin() + (CONTRIBUTING_FACTOR_VEHICLE_1);
    auto last = m_data.begin() + (CONTRIBUTING_FACTOR_VEHICLE_5 + 1);
    vector<string> cfs(first, last);

    // Removing duplicates
    sort(cfs.begin(), cfs.end());
    last = unique(cfs.begin(), cfs.end());
    cfs.erase(last, cfs.end());

    // Remove empty string or 'Unspecified' if any
    cfs.erase(std::remove_if(cfs.begin(), cfs.end(), filter), cfs.end());

    return cfs;
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
    string field, pushField("");
    bool no_quotes = true;

    m_data.clear();
    // Fixed a parsing error when meeting quoted fields thanks to https://stackoverflow.com/a/48086659/4442337
    while (getline(lineStream, field, ','))
    {
        if (static_cast<size_t>(count(field.begin(), field.end(), '"')) % 2 != 0)
        {
            no_quotes = !no_quotes;
        }

        pushField += field + (no_quotes ? "" : ",");

        if (no_quotes)
        {
            m_data.push_back(pushField);
            pushField.clear();
        }
    }
    // This checks for a trailing comma with no data after it.
    if (!lineStream && field.empty())
    {
        // If there was a trailing comma then add an empty element.
        m_data.push_back("");
    }
}

void CSVRow::readRowFromString(string line)
{
    stringstream lineStream(line);
    string field, pushField("");
    bool no_quotes = true;

    m_data.clear();
    // Fixed a parsing error when meeting quoted fields thanks to https://stackoverflow.com/a/48086659/4442337
    while (getline(lineStream, field, ','))
    {
        if (static_cast<size_t>(count(field.begin(), field.end(), '"')) % 2 != 0)
        {
            no_quotes = !no_quotes;
        }

        pushField += field + (no_quotes ? "" : ",");

        if (no_quotes)
        {
            m_data.push_back(pushField);
            pushField.clear();
        }
    }
    // This checks for a trailing comma with no data after it.
    if (!lineStream && field.empty())
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