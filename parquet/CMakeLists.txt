include_directories(${PROJECT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})

set(plugin parquet_input_format.cpp model/Field.cpp io/HDFSFileSource.cpp test.cpp model/Row.cpp model/Row.hpp)

if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
endif(LIBHDFS3_FOUND)

add_library(parquet-objs SHARED
        parquet_input_format.cpp)

set(husky husky-lib husky-io husky-core husky-base parquet-objs)

set(EXTERNAL_LIB ${EXTERNAL_LIB} ${PROJECT_SOURCE_DIR}/parquet_lib/libparquet.dylib)

# Test
add_executable(TestParquet test.cpp ${plugin})
#target_link_libraries(TestParquet ${husky})
target_link_libraries(TestParquet ${EXTERNAL_LIB})
set_property(TARGET TestParquet PROPERTY CXX_STANDARD 14)
