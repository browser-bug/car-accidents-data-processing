cmake_minimum_required(VERSION 3.6 FATAL_ERROR)

# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11 -fopenmp")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/OpenMP) # this sets the executables output directory

# list of file paths
set(UTILITY_PATH ${PROJECT_SOURCE_DIR}/utilities)

# list of source files
set(OMP_SOURCES 
  query1_monoread_omp.cc
  query2_monoread_omp.cc
  query3_monoread_omp.cc)

# include directories
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(${UTILITY_PATH}/include)
include_directories(${UTILITY_PATH}/csv_row/include)

# linking project libraries
link_libraries(utility csvrow)

# create binary files
add_executable(query1_monoread_omp.o query1_monoread_omp.cc)
add_executable(query2_monoread_omp.o query2_monoread_omp.cc)
add_executable(query3_monoread_omp.o query3_monoread_omp.cc)

