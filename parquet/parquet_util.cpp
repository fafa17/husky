//
// Created by Chris Liu on 21/12/2016.
//

#include <parquet/io/HDFSFileSource.hpp>
#include "parquet_util.hpp"

std::unique_ptr<::parquet::ParquetFileReader> husky::parquet::getHDFSParquetFileReader(hdfsFS *fs, const std::string *path) {
    HDFSFileSource* source = new HDFSFileSource();
    source->Open(*fs, *path);
    return ::parquet::ParquetFileReader::Open(std::unique_ptr<HDFSFileSource>(source));
}
