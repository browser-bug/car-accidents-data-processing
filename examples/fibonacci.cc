#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include "cpusec.h"

using namespace std;

//#pragma omp declare simd inbranch
int fib(int n) {  
  if (n <= 2) return n;
  else return fib(n-1) + fib(n-2);
}

int main(int argc, char **argv) {
  long int maxNum;
  
  maxNum = atol(argv[1]);

  long int *a = new long int[maxNum];
  long int *b = new long int[maxNum];
  

  for (int i=0; i < maxNum; i++) b[i] = i;

  double begin = cpuSecond();

  #pragma omp simd
  for (int i=0; i < maxNum; i++) {
    //a[i] = fib(b[i]);
    a[i] = b[i]*b[i];
  }
  
  double duration = cpuSecond()-begin;
  
  printf("fib(%ld)=%ld fib(%ld)=%ld\n", 10l, a[10], maxNum-1, a[maxNum-1]);
  printf("It took %fs to calculate it\n", duration);
}
