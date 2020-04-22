cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(MPI)

set(PROJECT_ROOT "/opt/car-accidents" CACHE STRING "Car-Accidents installation root directory")

# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}) # this sets the executables output directory

# required packages
find_package(MPI REQUIRED)

# list of file paths
set(UTILITY_PATH ${PROJECT_SOURCE_DIR}/../utils)

# list of source files
set(MPI_SOURCES 
  query1_monoread_mpi.cc
  query2_monoread_mpi.cc
  query3_monoread_mpi.cc)

# include directories
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(${UTILITY_PATH}/include)
include_directories(${UTILITY_PATH}/csv_row/include)

include_directories(${PROJECT_ROOT}/include)

# linking project libraries
link_libraries(${PROJECT_ROOT}/lib/libutility.so)
link_libraries(${PROJECT_ROOT}/lib/libcsvrow.so)

# linking MPICH
link_libraries(${MPI_CXX_LIBRARIES})
if(MPI_CXX_COMPILE_FLAGS)
  set_property(GLOBAL PROPERTY COMPILE_FLAGS "${MPI_CXX_COMPILE_FLAGS}")
endif()
if(MPI_CXX_LINK_FLAGS)
set_property(GLOBAL PROPERTY LINK_FLAGS "${MPI_CXX_LINK_FLAGS}")
endif()

# create binary files
add_executable(query1_monoread_mpi.o query1_monoread_mpi.cc)
add_executable(query2_monoread_mpi.o query2_monoread_mpi.cc)
add_executable(query3_monoread_mpi.o query3_monoread_mpi.cc)

