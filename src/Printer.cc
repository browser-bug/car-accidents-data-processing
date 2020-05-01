#include "Printer.h"

using namespace std;

void Printer::writeOutput(int data[][NUM_WEEKS_PER_YEAR])
{
    outFile << "********* QUERY 1 *********" << '\n';
    int totalWeeks = 0;
    int totalAccidents = 0;

    for (int year = 0; year < NUM_YEARS; year++)
    {
        for (int week = 0; week < NUM_WEEKS_PER_YEAR; week++)
        {
            int numLethAcc = data[year][week];
            if (numLethAcc > 0)
            {
                outFile << "(" << (year + 2012) << ")Week: " << (week + 1) << "\t\t\t Num. lethal accidents: ";
                outFile << numLethAcc << '\n';
                totalAccidents += numLethAcc;
                totalWeeks++;
            }
        }
    }
    outFile << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << "\n\n\n";
}

void Printer::writeOutput(AccPair data[NUM_BOROUGH])
{
    outFile << "********* QUERY 2 *********" << '\n';
    for (auto el : dictQuery2)
    {
        double perc = double(data[el.second].numLethalAccidents) / data[el.second].numAccidents;
        outFile << el.first << '\n'
                << "\t\tNum. of accidents: " << data[el.second].numAccidents
                << "\t\t\t\t\tPerc. lethal accidents: " << setprecision(4) << fixed << perc * 100 << "%"
                << '\n';
    }
    outFile << "\nTotal contributing factors parsed: " << dictQuery2.size() << "\n\n\n";
}

void Printer::writeOutput(AccPair data[][NUM_YEARS][NUM_WEEKS_PER_YEAR])
{
    outFile << "********* QUERY 3 *********" << '\n';
    for (auto b : dictQuery3)
    {
        int numWeeks = 0;
        int numAccidents = 0;
        double numLethalAccidents = 0;

        outFile << "Borough: " << b.first << '\n';
        for (int year = 0; year < NUM_YEARS; year++)
        {
            for (int week = 0; week < NUM_WEEKS_PER_YEAR; week++)
            {
                if (data[b.second][year][week].numAccidents == 0)
                    continue;
                numWeeks++;
                numAccidents += data[b.second][year][week].numAccidents;
                numLethalAccidents += data[b.second][year][week].numLethalAccidents;

                outFile << "(" << (year + 2012) << ")Week " << (week + 1);                               // print (Year)Week N
                outFile << "\t\t\t num. accidents: " << data[b.second][year][week].numAccidents << '\n'; // print numAccidents
            }
        }
        double avg = numLethalAccidents / numWeeks;
        outFile << "[" << b.first << "] Avg. lethal accidents per week is: " << setprecision(2) << fixed << avg * 100 << "%";
        outFile << "\t\t\tNum. accidents: " << numAccidents << "\t\tNum. lethal accidents: " << setprecision(0) << fixed << numLethalAccidents << '\n';
        outFile << '\n';
    }
    outFile << "Total boroughs parsed: " << dictQuery3.size() << "\n\n\n";
}