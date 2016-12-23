//
// Created by Chris Liu on 21/12/2016.
//

#include <parquet/io/HDFSFileSource.hpp>
#include <base/exception.hpp>
#include "parquet_util.hpp"

std::unique_ptr<::parquet::ParquetFileReader> husky::parquet::getHDFSParquetFileReader(hdfsFS *fs, const std::string *path) {
    HDFSFileSource* source = new HDFSFileSource();
    source->Open(*fs, *path);
    return ::parquet::ParquetFileReader::Open(std::unique_ptr<HDFSFileSource>(source));
}

int husky::parquet::getIndByColName(const ::parquet::SchemaDescriptor& descriptor, const std::string& col_name){
    for( int x = 0; x < descriptor.num_columns(); x++){
        if ( descriptor.Column(x)->name() == col_name)
            return x;
    }
    throw husky::base::HuskyException("Column is not exist");
}
