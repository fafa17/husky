//
// Created by Chris Liu on 21/12/2016.
//

#ifndef PROJECT_PARQUET_UTIL_HPP
#define PROJECT_PARQUET_UTIL_HPP

#include <parquet/file/reader.h>
#include <hdfs/hdfs.h>

namespace husky{
    namespace parquet{
        std::unique_ptr<::parquet::ParquetFileReader> getHDFSParquetFileReader(hdfsFS* fs, const std::string* path);
    }
}

#endif //PROJECT_PARQUET_UTIL_HPP
