#include <omp.h>
#include <iostream>
#include <sstream>
#include <string>
#include <stdio.h>

using namespace std;

#define NUMELEM 100

int main() {	
  int *data;
  //int data[NUMELEM]
  data = new int[NUMELEM];
  
#pragma omp parallel
  {
    for(int i=0; i<NUMELEM; i++) {
      data[i]=omp_get_thread_num();
    }
    int tot=0;
    for(int i=0; i<NUMELEM; i++) {
      tot += data[i];
    }
    printf("Thread %d calculated %d\n", omp_get_thread_num(), tot);
  }  // barrier here
}
