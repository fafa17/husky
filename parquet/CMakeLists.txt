include_directories(${PROJECT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})

add_library(Parquet parquet_input_format.cpp parquet_file_assigner.cpp)

if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
endif(LIBHDFS3_FOUND)

if(MONGOCLIENT_FOUND)
    include_directories(${MONGOCLIENT_INCLUDE_DIR})
    add_definitions(-DWITH_MONGODB)
endif(MONGOCLIENT_FOUND)

add_library(parquet-objs OBJECT
        parquet_file_assigner.cpp
        parquet_input_format.cpp)

set(husky husky-lib husky-io husky-core husky-base)

# Test
add_executable(TestParquet parquet_input_format.cpp)
target_link_libraries(TestParquet ${husky})
set_property(TARGET TestParquet PROPERTY CXX_STANDARD 14)
