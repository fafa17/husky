//
// Created by Chris Liu on 8/11/2016.
//

#include "parquet_input_format.hpp"
#include "parquet_util.hpp"
#include "io/ParquetSplit.hpp"

#include <hdfs/hdfs.h>
#include <parquet/file/metadata.h>
#include <parquet/column/scan-all.h>
#include <parquet/types.h>
#include <parquet/io/HDFSFileSource.hpp>

#include <regex>
#include <iostream>

#include <io/hdfs_manager.hpp>
#include <parquet/model/Row.hpp>
#include <core/context.hpp>
#include <core/constants.hpp>

void husky::io::ParquetInputFormat::setRowGroup(hdfsFS fs, std::string filePath, int32_t rowgroupid) {

    //Realod parquet reader and rowGroupReader
    //Set schema
    current_file_reader = husky::parquet::getHDFSParquetFileReader(&fs, &filePath);

    current_row_group_reader = current_file_reader->RowGroup(rowgroupid);

    schema = current_file_reader->metadata()->schema();

    convertToRow();

    row_buffer_counter = 0;
}

bool husky::io::ParquetInputFormat::next(husky::io::ParquetInputFormat::RecordT& row) {
    if(row_buffer == nullptr)
        if(!nextRowGroup()){
            return false;
        }
    // check the row count
    // check if there are next row group from assigner
    if(row_buffer_counter == row_buffer->size()){
        if(!nextRowGroup()){
            return false;
        }
    }
    row.set(row_buffer->at(row_buffer_counter).get());
    row_buffer_counter++;
    return true;
}

int husky::pt::GetTypeByteSize(::parquet::Type::type parquet_type) {
    switch (parquet_type) {
        case Type::BOOLEAN:
            return type_traits<BooleanType::type_num>::value_byte_size;
        case Type::INT32:
            return type_traits<Int32Type::type_num>::value_byte_size;
        case Type::INT64:
            return type_traits<Int64Type::type_num>::value_byte_size;
        case Type::INT96:
            return type_traits<Int96Type::type_num>::value_byte_size;
        case Type::DOUBLE:
            return type_traits<DoubleType::type_num>::value_byte_size;
        case Type::FLOAT:
            return type_traits<FloatType::type_num>::value_byte_size;
        case Type::BYTE_ARRAY:
            return type_traits<ByteArrayType::type_num>::value_byte_size;
        case Type::FIXED_LEN_BYTE_ARRAY:
            return type_traits<FLBAType::type_num>::value_byte_size;
        default:
            return 0;
    }
}

void husky::io::ParquetInputFormat::convertToRow() {
    auto total_row = getNumOfRow();
    auto total_column = getNumOfColumn();

    //clean up
    row_buffer = new std::vector<Row>(total_row);

    int64_t values_read = 0;
    uint8_t** values = new uint8_t*[total_column];

    // loop all value and read to buffer
    for ( int x = 0; x < total_column; x++){
        size_t value_byte_size = pt::GetTypeByteSize(current_row_group_reader->Column(x)->descr()->physical_type());
        values[x] = new uint8_t[total_row * value_byte_size];
        ::parquet::ScanAllValues(total_row, nullptr, nullptr, values[x], &values_read, current_row_group_reader->Column(x).get());
        values[x] = std::move(values[x]);
    }

    // from the buffer, construct the Row object
    for ( int x = 0; x < total_row; x++){
        std::vector<Field>* fields = new std::vector<Field>(total_column);
        for ( int y = 0; y< total_column; y++){
            size_t value_byte_size = pt::GetTypeByteSize(current_row_group_reader->Column(y)->descr()->physical_type());
            fields->at(y).set((void *)(&values[y][x * value_byte_size]));
        }
        row_buffer->at(x).set(fields);
    }
}

bool husky::io::ParquetInputFormat::nextRowGroup() {

    //RPC to master for asking ParquetSplit
    //Request: file_url
    //Respond: ParquetSplite if rowgroupud = -1 <-- all splits are processed
    BinStream question;
    question << current_file << husky::Context::get_param("hostname");
    BinStream answer = husky::Context::get_coordinator().ask_master(question, husky::TYPE_PARQUET_BLK_REQ);
    std::string fn;
    answer >> fn;
    ParquetSplit* split = new ParquetSplit();
    split->fromString(fn);

    //check if the split is valid
    if(split->rowgroup_id == -1){
        //no tasks
        return false;
    }

    setRowGroup(husky::io::HDFSManager::getInstance()->get_fs(), split->path, split->rowgroup_id);

    return true;
}
