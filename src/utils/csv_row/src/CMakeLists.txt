# compilation flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")

# list of source files
set(CSV_ROW_SRC CSVRow.cc)

# build for convenience library
add_library (csvrow ${CSV_ROW_SRC})

# includes
target_include_directories(csvrow PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_include_directories(csvrow PUBLIC ${PROJECT_SOURCE_DIR}/csv_row/include)

# links
target_link_libraries(csvrow PUBLIC utility)
