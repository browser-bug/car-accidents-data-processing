#include <omp.h>
#include <iostream>
#include <sstream>

using namespace std;

int main()
{
    int i, a = 2;
    int n = 3;
#pragma omp parallel num_threads(3) private(i, a)
    {
        printf("a variable first is: %d\n", a);
        for (i = 0; i < n; i++)
        {
            a = i + omp_get_thread_num();
            printf("Thread %d has a value of a=%d for i=%d\n",
                   omp_get_thread_num(), a, i);
        } /*-- End of parallel for --*/
    }
    printf("final value of 'a' is : %d\n", a);
}
