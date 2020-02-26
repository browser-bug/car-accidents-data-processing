#include <omp.h>
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <string>

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

    // CSVRow row;
    // file >> row >> row;

    // Number of lethal accidents per week throughout the entire dataset. Using index: NUMBER_OF_PERSONS_KILLED
    for (CSVIterator loop(file); loop != CSVIterator(); ++loop)
    {
        CSVRow row = (*loop);
        int num_pers_killed = row.getNumPersonKilled();

        if (num_pers_killed > 0)
        {
            row.print();
            cout << endl;
        }
    }
}