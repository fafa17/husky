include_directories(${PROJECT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})

add_library(Parquet parquet_input_format.cpp parquet_file_assigner.cpp model/Field.cpp model/Field.hpp io/HDFSFileSource.cpp io/HDFSFileSource.hpp test.cpp test.hpp)

if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
endif(LIBHDFS3_FOUND)

add_library(parquet-objs SHARED
        parquet_input_format.cpp)

set(husky husky-lib husky-io husky-core husky-base parquet-objs)

# Test
add_executable(TestParquet test.cpp)
target_link_libraries(TestParquet ${husky})
set_property(TARGET TestParquet PROPERTY CXX_STANDARD 14)
