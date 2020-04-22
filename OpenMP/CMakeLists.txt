cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(OpenMP)

# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11 -fopenmp")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}) # this sets the executables output directory

set(PROJECT_ROOT "/opt/car-accidents" CACHE STRING "Car-Accidents installation root directory")

# required packages
find_package(OpenMP REQUIRED)

# list of file paths
set(UTILITY_PATH ${PROJECT_SOURCE_DIR}/../utils)

# list of source files
set(OMP_SOURCES 
  query1_monoread_omp.cc
  query2_monoread_omp.cc
  query3_monoread_omp.cc)

# include directories
include_directories(${UTILITY_PATH}/include)
include_directories(${UTILITY_PATH}/csv_row/include)

include_directories(${PROJECT_ROOT}/include)

# linking project libraries
link_libraries(${PROJECT_ROOT}/lib/libutility.so)
link_libraries(${PROJECT_ROOT}/lib/libcsvrow.so)

# create binary files
add_executable(query1_monoread_omp.o query1_monoread_omp.cc)
add_executable(query2_monoread_omp.o query2_monoread_omp.cc)
add_executable(query3_monoread_omp.o query3_monoread_omp.cc)

