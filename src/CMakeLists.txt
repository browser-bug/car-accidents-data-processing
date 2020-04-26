# work on libraries first
add_subdirectory(utils)

# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11 -fopenmp")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin) # this sets the executables output directory

# list source files
set(SRC_FILES
  Loader.cc
  Scatterer.cc
  Process.cc
  Printer.cc
  Stats.cc
)

# required packages
find_package(MPI REQUIRED)
find_package(OpenMP REQUIRED)

# list of file paths
set(UTILITY_PATH ${PROJECT_SOURCE_DIR}/src/utils)

# include directories
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(${UTILITY_PATH}/include)
include_directories(${UTILITY_PATH}/csv_row/include)
include_directories(${PROJECT_SOURCE_DIR}/include)

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
add_executable (allQueries_multiread.o allQueries_multiread.cc ${SRC_FILES})
add_executable (allQueries_monoread.o allQueries_monoread.cc ${SRC_FILES})
add_executable (allQueries_serial.o allQueries_serial.cc ${SRC_FILES})
# add_executable(test.o test.cc ${SRC_FILES})