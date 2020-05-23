#include "Printer.h"

using namespace std;

void Printer::writeLethAccPerWeek(int *data)
{
    outFile << "********* QUERY 1 *********" << '\n';
    int totalWeeks = 0;
    int totalAccidents = 0;

    for (int year = 0; year < numYears; year++)
    {
        for (int week = 0; week < numWeeksPerYear; week++)
        {
            int numLethAcc = data[(year * numWeeksPerYear) + week];
            if (numLethAcc > 0)
            {
                outFile << "(" << (year + BASE_YEAR) << ")Week: " << (week + 1) << "\t\t\t Num. lethal accidents: ";
                outFile << numLethAcc << '\n';
                totalAccidents += numLethAcc;
                totalWeeks++;
            }
        }
    }
    outFile << "Total weeks: " << totalWeeks << "\t\t\tTotal accidents: " << totalAccidents << "\n\n\n";
}

void Printer::writeNumAccAndPerc(AccPair *data)
{
    outFile << "********* QUERY 2 *********" << '\n';
    for (auto cf : dictQuery2)
    {
        AccPair ap = data[cf.second];
        double perc = double(ap.numLethalAccidents) / ap.numAccidents;
        outFile << cf.first << '\n'
                << "\t\tNum. of accidents: " << ap.numAccidents
                << "\t\t\t\t\tPerc. lethal accidents: " << setprecision(4) << fixed << perc * 100 << "%"
                << '\n';
    }
    outFile << "\nTotal contributing factors parsed: " << dictQuery2.size() << "\n\n\n";
}

void Printer::writeBoroughWeekAcc(AccPair *data)
{
    outFile << "********* QUERY 3 *********" << '\n';
    for (auto brgh : dictQuery3)
    {
        int numWeeks = 0;
        int numTotAccidents = 0;
        double numTotLethalAccidents = 0;

        outFile << "Borough: " << brgh.first << '\n';
        for (int year = 0; year < numYears; year++)
        {
            for (int week = 0; week < numWeeksPerYear; week++)
            {
                AccPair ap = data[(brgh.second * numYears * numWeeksPerYear) + (year * numWeeksPerYear) + week];
                if (ap.numAccidents == 0)
                    continue;
                int n_acc = ap.numAccidents;
                double num_lethacc = ap.numLethalAccidents;

                numWeeks++;
                numTotAccidents += n_acc;
                numTotLethalAccidents += num_lethacc;

                // print (Year)Week N
                outFile << "(" << (year + BASE_YEAR) << ")Week " << (week + 1) << "\n";
                // print numAccidents and avgLethalAccidents
                outFile << "\t num. accidents: " << n_acc << "\n"
                        << "\t avg lethal accidents: " << setprecision(4) << fixed << (num_lethacc / n_acc) * 100 << "%"
                        << '\n';
            }
        }
        double avg = numTotLethalAccidents / numWeeks;
        outFile << "[" << brgh.first << "] Lethal accidents per week is: " << setprecision(2) << fixed << avg * 100 << "%";
        outFile << "\t\t\tNum. accidents: " << numTotAccidents << "\t\tNum. lethal accidents: " << setprecision(0) << fixed << numTotLethalAccidents << '\n';
        outFile << '\n';
    }
    outFile << "Total boroughs parsed: " << dictQuery3.size() << "\n\n\n";
}