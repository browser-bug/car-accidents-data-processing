# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")

# list of source files
set(utils_SRC utility.cc)

# build for convenience library
add_library(utility SHARED ${utils_SRC})

# includes
target_include_directories(utility PUBLIC ${PROJECT_SOURCE_DIR}/utils/include)

# installation
install(TARGETS utility DESTINATION ${PROJECT_ROOT}/lib)