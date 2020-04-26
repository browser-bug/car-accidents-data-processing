#include "Printer.h"

using namespace std;

void Printer::writeQuery1(int data[][NUM_WEEKS_PER_YEAR])
{
    outFile << "********* QUERY 1 *********" << endl;
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
                outFile << numLethAcc << endl;
                totalAccidents += numLethAcc;
                totalWeeks++;
            }
        }
    }
    outFile << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << "\n\n\n";
}

void Printer::writeQuery2(AccPair data[NUM_BOROUGH])
{
    outFile << "********* QUERY 2 *********" << endl;
    for (auto el : dictQuery2)
    {
        double perc = double(data[el.second].numLethalAccidents) / data[el.second].numAccidents;
        outFile << el.first << endl
                << "\t\tNum. of accidents: " << data[el.second].numAccidents
                << "\t\t\t\t\tPerc. lethal accidents: " << setprecision(4) << fixed << perc * 100 << "%"
                << endl;
    }
    outFile << "\nTotal contributing factors parsed: " << dictQuery2.size() << "\n\n\n";
}

void Printer::writeQuery3(AccPair data[][NUM_YEARS][NUM_WEEKS_PER_YEAR])
{
    outFile << "********* QUERY 3 *********" << endl;
    for (auto b : dictQuery3)
    {
        int numWeeks = 0;
        int numAccidents = 0;
        double numLethalAccidents = 0;

        outFile << "Borough: " << b.first << endl;
        for (int i = 0; i < NUM_YEARS; i++) // for each year
        {
            for (int j = 0; j < NUM_WEEKS_PER_YEAR; j++) // for each week
            {
                if (data[b.second][i][j].numAccidents == 0)
                    continue;
                numWeeks++;
                numAccidents += data[b.second][i][j].numAccidents;
                numLethalAccidents += data[b.second][i][j].numLethalAccidents;

                outFile << "(" << (i + 2012) << ")Week " << (j + 1);                               // print (Year)Week N
                outFile << "\t\t\t num. accidents: " << data[b.second][i][j].numAccidents << endl; // print numAccidents
            }
        }
        double avg = numLethalAccidents / numWeeks;
        outFile << "[" << b.first << "] Avg. lethal accidents per week is: " << setprecision(2) << fixed << avg * 100 << "%";
        outFile << "\t\t\tNum. accidents: " << numAccidents << "\t\tNum. lethal accidents: " << setprecision(0) << fixed << numLethalAccidents << endl;
        outFile << endl;
    }
    outFile << "Total boroughs parsed: " << dictQuery3.size() << "\n\n\n";
}