include_directories(${PROJECT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})

set(plugin parquet_input_format.cpp model/Field.cpp io/HDFSFileSource.cpp test.cpp model/Row.cpp model/Row.hpp parquet_util.cpp parquet_util.hpp io/ParquetSplit.cpp io/ParquetSplit.hpp)


if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
endif(LIBHDFS3_FOUND)

set(husky husky-lib husky-io husky-core husky-base)

set(EXTERNAL_LIB ${EXTERNAL_LIB} ${PROJECT_SOURCE_DIR}/parquet_lib/libparquet.dylib)

# Test
add_executable(TestParquet test.cpp ${plugin})
target_link_libraries(TestParquet ${husky})
target_link_libraries(TestParquet ${EXTERNAL_LIB})
set_property(TARGET TestParquet PROPERTY CXX_STANDARD 14)

add_library(parquet-util-object OBJECT parquet_util.cpp io/HDFSFileSource.cpp io/ParquetSplit.cpp)
add_library(husky-parquet-util STATIC $<TARGET_OBJECTS:parquet-util-object>)

add_library(parquet-obj OBJECT ${plugin})
set(parquet-obj ${parquet-obj} PARENT_SCOPE)
add_library(husky-parquet STATIC $<TARGET_OBJECTS:parquet-obj>)
