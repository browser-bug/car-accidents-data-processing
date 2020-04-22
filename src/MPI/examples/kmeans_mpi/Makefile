DEBUG = 
C_OMP = -fopenmp

CC=gcc
CFLAGS = -march=native -mtune=native -ftree-vectorize -Ofast -Wall $(DEBUG)
CXX=g++
CXXFLAGS = -march=native -mtune=native -ftree-vectorize -Ofast -std=c++11 -Wall $(DEBUG)

MPICC=mpicc
MPICXX=mpic++

OBJS = 

LIBS = -lm

TARGET = kmeans kmeans_serial

all: $(TARGET)

kmeans: $(OBJS) kmeans.o
	 $(MPICXX) $(C_OMP) -o kmeans $(OBJS) kmeans.o $(LIBS)

kmeans_serial: $(OBJS) kmeans_serial.o
	 $(MPICXX) $(C_OMP) -o kmeans_serial $(OBJS) kmeans_serial.o $(LIBS)

clean:
	rm -rf *.o $(TARGET)

kmeans.o : kmeans.cc
	$(MPICXX) $(CFLAGS) $(C_OMP) -c kmeans.cc -o kmeans.o

kmeans_serial.o : kmeans_serial.cc
	$(MPICXX) $(CFLAGS) $(C_OMP) -c kmeans_serial.cc -o kmeans_serial.o

