#include <omp.h>
#include <iostream>
#include <sstream>

using namespace std;

int main()
{
    int n = 3;
    int i = 0;
#pragma omp parallel num_threads(3) reduction(+ : i)
    {
        for (int k = 0; k < n; k++)
        {
            i++;
        } /*-- End of parallel for --*/
    }
    printf("final value of 'i' is : %d\n", i);
}
