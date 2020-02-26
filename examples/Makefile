DEBUG = 
C_OMP = -fopenmp 

CC=gcc
CXX=g++
CXXFLAGS = -march=native -mtune=native -ftree-vectorize -Ofast -std=c++11 -Wall $(DEBUG)


OBJS = cpusec.o

LIBS = 

TARGET = primes fibonacci parallel

all: $(TARGET)

primes: $(OBJS) primes.o
	 $(CXX) $(C_OMP) -o primes $(OBJS) primes.o $(LIBS)

fibonacci: $(OBJS) fibonacci.o
	 $(CXX) $(C_OMP) -o fibonacci $(OBJS) fibonacci.o $(LIBS)

parallel: $(OBJS) parallel.o
	 $(CXX) $(C_OMP) -o parallel $(OBJS) parallel.o $(LIBS)

clean:
	rm -rf *.o $(TARGET)

primes.o : cpusec.h primes.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c primes.cc -o primes.o

fibonacci.o : cpusec.h fibonacci.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c fibonacci.cc -o fibonacci.o

parallel.o : parallel.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c parallel.cc -o parallel.o

cpusec.o : cpusec.h cpusec.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c cpusec.cc -o cpusec.o

