#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include "cpusec.h"

using namespace std;

bool isPrime(long int x) {
  long int i;

  for(i=2; i<=x/2; i++) {
    if(x%i==0) return false;
  }
  return true;
}

int main(int argc, char **argv) {
  long int maxNum, count;
  
  maxNum = atol(argv[1]);

  double begin = cpuSecond();
  count = 0;

  #pragma omp parallel for reduction(+:count) schedule(dynamic, 1)
  for(long int i=1; i<=maxNum; i++) {
      if(isPrime(i)) count++;
  }
  
  double duration = cpuSecond()-begin;
  
  printf("The number of primes between 1 and %ld is %ld\n", maxNum, count);
  printf("It took %fs to calculate it\n", duration);
}
