#include <omp.h>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>
#include <time.h>

#include "utilities/CSVIterator.h"

using namespace std;

int main()
{
    string csv_path = "dataset/data_test.csv";
    ifstream file(csv_path);

    // Print the entire dataset
    // CSVRow row;
    // for (CSVIterator loop(file); loop != CSVIterator(); loop++)
    // {
    //     row = (*loop);
    //     row.print();
    //     cout << endl;
    // }

    // map<string, int> accPerDay; // map { Day ; num_accidents }

    // // Number of lethal accidents per week throughout the entire dataset. Using index: NUMBER_OF_PERSONS_KILLED
    // for (CSVIterator loop(file); loop != CSVIterator(); ++loop)
    // {
    //     CSVRow row = (*loop);
    //     int num_pers_killed = row.getNumPersonKilled();

    //     if (num_pers_killed > 0)
    //     {
    //         auto it = accPerDay.find(row[DATE]);
    //         if (it != accPerDay.end())
    //             it->second += num_pers_killed;
    //         else
    //             accPerDay.insert({row[DATE], num_pers_killed});
    //     }
    // }

    // typedef map<string, int>::const_iterator MapIterator;
    // for (MapIterator iter = accPerDay.begin(); iter != accPerDay.end(); iter++)
    // {
    //     cout << "Key: " << iter->first << endl
    //          << "Values: ";
    //     cout << iter->second << endl;
    //     // typedef int ::const_iterator ListIterator;
    //     // for (ListIterator list_iter = iter->second.begin(); list_iter != iter->second.end(); list_iter++)
    //     //     cout << " " << *list_iter << endl;
    // }
}