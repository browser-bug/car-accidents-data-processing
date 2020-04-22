#include <omp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include <algorithm>
#include <string>

#include "utils/csv_row/CSVIterator.h"

#define ORIGINAL_SIZE 955928
#define TEST_SIZE 29999

#define NUM_YEARS 6
#define NUM_WEEKS_PER_YEAR 53

#define NUM_CONTRIBUTING_FACTORS 47

using namespace std;

int main()
{
    int ans = 4;
#pragma omp parallel for firstprivate(ans)
    for (int i = 0; i < 10; i++)
    {
        cout << ans << endl;
    }
}