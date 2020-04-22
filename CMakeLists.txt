cmake_minimum_required(VERSION 3.2 FATAL_ERROR)
project(car-accidents-data-processing)

set(OPT_DIR "/opt/car-accidents" CACHE STRING "Car-Accidents installation root directory")

# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11 -fopenmp")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin) # this sets the executables output directory

# add_subdirectory(include)
add_subdirectory(src)