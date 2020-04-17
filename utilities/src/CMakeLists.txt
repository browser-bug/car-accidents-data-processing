# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")

# list of source files
set(UTILITIES_SRC utility.cc)

# build for convenience library
add_library(utility SHARED ${UTILITIES_SRC})
set_target_properties(utility PROPERTIES
            ARCHIVE_OUTPUT_DIRECTORY "lib"
            LIBRARY_OUTPUT_DIRECTORY "lib"
            OUTPUT_NAME utility)

# includes
target_include_directories(utility PUBLIC ${PROJECT_SOURCE_DIR}/utilities/include)

# installation
include(GNUInstallDirs)
install(TARGETS utility
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})