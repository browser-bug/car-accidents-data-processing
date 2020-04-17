# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")

# list of source files
set(CSV_ROW_SRC CSVRow.cc)

# build for convenience library
add_library (csvrow SHARED ${CSV_ROW_SRC})
set_target_properties(csvrow PROPERTIES
            ARCHIVE_OUTPUT_DIRECTORY "lib"
            LIBRARY_OUTPUT_DIRECTORY "lib"
            OUTPUT_NAME csvrow)

# includes
target_include_directories(csvrow PUBLIC ${PROJECT_SOURCE_DIR}/utilities/include)
target_include_directories(csvrow PUBLIC ${PROJECT_SOURCE_DIR}/utilities/csv_row/include)

# links
target_link_libraries(csvrow PUBLIC utility)

# installation
include(GNUInstallDirs)
install(TARGETS csvrow
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})