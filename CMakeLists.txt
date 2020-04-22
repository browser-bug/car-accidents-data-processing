cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(car-accidents-data-processing)

set(PROJECT_ROOT "/opt/car-accidents" CACHE STRING "Car-Accidents installation root directory")

# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11 -fopenmp")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}) # this sets the executables output directory

# required packages
find_package(MPI REQUIRED)
find_package(OpenMP REQUIRED)

# work on libraries first
add_subdirectory(utils)

# list of file paths
set(UTILITY_PATH ${PROJECT_SOURCE_DIR}/utils)

# include directories
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(${UTILITY_PATH}/include)
include_directories(${UTILITY_PATH}/csv_row/include)

# linking project libraries
link_libraries(utility csvrow)

# linking MPICH
link_libraries(${MPI_CXX_LIBRARIES})
if(MPI_CXX_COMPILE_FLAGS)
  set_property(GLOBAL PROPERTY COMPILE_FLAGS "${MPI_CXX_COMPILE_FLAGS}")
endif()
if(MPI_CXX_LINK_FLAGS)
set_property(GLOBAL PROPERTY LINK_FLAGS "${MPI_CXX_LINK_FLAGS}")
endif()

# create binary files
add_executable (allQueries_multiread.o allQueries_multiread.cc)
add_executable (allQueries_monoread.o allQueries_monoread.cc)
add_executable (allQueries_serial.o allQueries_serial.cc)