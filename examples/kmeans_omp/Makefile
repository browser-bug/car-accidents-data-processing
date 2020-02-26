DEBUG = 
C_OMP = -fopenmp

CC=gcc
CXX=g++
CXXFLAGS = -march=native -mtune=native -ftree-vectorize -Ofast -std=c++11 -Wall $(DEBUG)

OBJS = 

LIBS = 

TARGET = kmeans_serial kmeans_optimized kmeans_noatomic kmeans_noatomic2 kmeans_noatomic_ale kmeans_par

all: $(TARGET)

kmeans_serial: $(OBJS) kmeans_serial.o
	 $(CXX) -o kmeans_serial $(OBJS) kmeans_serial.o $(LIBS)

kmeans_optimized: $(OBJS) kmeans_optimized.o
	 $(CXX) $(C_OMP) -o kmeans_optimized $(OBJS) kmeans_optimized.o $(LIBS)

kmeans_noatomic: $(OBJS) kmeans_noatomic.o
	 $(CXX) $(C_OMP) -o kmeans_noatomic $(OBJS) kmeans_noatomic.o $(LIBS)

kmeans_noatomic2: $(OBJS) kmeans_noatomic2.o
	 $(CXX) $(C_OMP) -o kmeans_noatomic2 $(OBJS) kmeans_noatomic2.o $(LIBS)

kmeans_noatomic_ale: $(OBJS) kmeans_noatomic_ale.o
	 $(CXX) $(C_OMP) -o kmeans_noatomic_ale $(OBJS) kmeans_noatomic_ale.o $(LIBS)

kmeans_par: $(OBJS) kmeans_par.o
	 $(CXX) $(C_OMP) -o kmeans_par $(OBJS) kmeans_par.o $(LIBS)


clean:
	rm -rf *.o $(TARGET)

kmeans_serial.o : kmeans_serial.cc
	$(CXX) $(CXXFLAGS) -c kmeans_serial.cc -o kmeans_serial.o

kmeans_optimized.o : kmeans_optimized.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c kmeans_optimized.cc -o kmeans_optimized.o

kmeans_noatomic.o : kmeans_noatomic.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c kmeans_noatomic.cc -o kmeans_noatomic.o

kmeans_noatomic2.o : kmeans_noatomic2.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c kmeans_noatomic2.cc -o kmeans_noatomic2.o

kmeans_noatomic_ale.o : kmeans_noatomic_ale.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c kmeans_noatomic_ale.cc -o kmeans_noatomic_ale.o

kmeans_par.o : kmeans_par.cc
	$(CXX) $(CXXFLAGS) $(C_OMP) -c kmeans_par.cc -o kmeans_par.o


