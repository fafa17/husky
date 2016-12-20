//
// Created by Chris Liu on 8/11/2016.
//

#include "parquet_input_format.hpp"

#include <hdfs/hdfs.h>
#include <parquet/file/metadata.h>
#include <parquet/column/scan-all.h>
#include <parquet/types.h>
#include <parquet/io/HDFSFileSource.hpp>

#include <regex>
#include <iostream>

#include <io/hdfs_manager.hpp>
#include <parquet/model/Row.hpp>

const int _parquet_magic_len = 4;
const int _parquet_footer_size_len = 4;

/**
 * To recursive find all files in a directory
 * @param fileList
 * @param fs
 * @param url
 * @param fileFilter
 */
void recursiveHdfsDirectoryList(std::vector<hdfsFileInfo>* fileList, hdfsFS fs, const std::string& url, const std::regex& fileFilter) {

    int num_files = 0;
    auto* file_info_list = hdfsListDirectory(fs, url.c_str(), &num_files);

    for (int x = 0; x < num_files; x++) {
        if(file_info_list[x].mKind == kObjectKindDirectory){
            recursiveHdfsDirectoryList(fileList, fs, url + std::string("/") + file_info_list[x].mName, fileFilter);
        } else {
            if(std::regex_match(file_info_list[x].mName, fileFilter))
                fileList->push_back(file_info_list[x]);
        }
    }
}

///**
// * To get the size of the footer of a Parquet file
// * @param file
// * @param info
// * @param fs
// * @return
// */
//int32_t getParquetFooterSize(hdfsFile file, hdfsFileInfo* info, hdfsFS fs){
//    auto filesize = info->mSize;
//    int64_t footer_size_start = filesize - _parquet_footer_size_len - _parquet_magic_len;
//
//    //Footer Size from the hdfs block
//    int32_t* footer_size;
//
//    hdfsPread(fs, file, footer_size_start, footer_size, _parquet_footer_size_len);
//}
//
//
//parquet::FileMetaData* getParquetFooter(std::string& url, hdfsFS fs) {
//    auto *file_info = hdfsGetPathInfo(fs, url.c_str());
//    auto file_size = file_info->mSize;
//
//    auto hdfs_file = hdfsOpenFile(fs, url.c_str(), O_RDONLY, 0, 0, 0);
//    auto footer_size = getParquetFooterSize(hdfs_file, file_info, fs);
//
////    parquet::FileMetaData::Make(&metadata_buffer[0], &footer_size);
//}
////
////bool husky::io::ParquetLoader::load(const std::string& url) {
////    //hadoop read
////    //list all files
////    std::regex parquetSuffixFilter("*.parquet");
////    auto * file_list = new std::vector<hdfsFileInfo>();
////    recursiveHdfsDirectoryList(file_list, this->hdfs, url, parquetSuffixFilter);
////
////    //get footers of all files
////    //TODO: checking schema consistency over files
////    for ( int x = 0; x < file_list->size(); x++ ){
////
////    }
////    //Create new split object for each row group
////    //set to assigner
////    return false;
////}

void husky::io::ParquetInputFormat::set(std::string filePath, int64_t startPos, int64_t len) {
    current_file = filePath;
    current_start_pos = startPos;
    current_len = len;

    //Realod parquet reader and rowGroupReader
    //Set schema
    auto source = new HDFSFileSource();
    source->Open(HDFS::getManager()->get_fs(), filePath);
    current_file_reader = parquet::ParquetFileReader::Open(std::unique_ptr<RandomAccessSource>(source));

    current_row_group_reader = current_file_reader->RowGroup(1);

    schema = current_file_reader->metadata()->schema();

    convertToRow();

    isSetup = true;
}

//TBD
void husky::io::ParquetInputFormat::setLocal(std::string filePath, int64_t startPos, int64_t len) {
    current_file = filePath;
    current_start_pos = startPos;
    current_len = len;

    //Realod parquet reader and rowGroupReader
    //Set schema
    auto source = new LocalFileSource();
    source->Open(filePath);
    current_file_reader = parquet::ParquetFileReader::Open(std::unique_ptr<RandomAccessSource>(source));

    current_row_group_reader = current_file_reader->RowGroup(0);

    schema = current_file_reader->metadata()->schema();

    convertToRow();

    isSetup = true;
}

bool husky::io::ParquetInputFormat::next(husky::io::ParquetInputFormat::RecordT &) {
    // Scan all row in the row group
//
//
//    //load a row group into the ram
//
//
//    for (int64_t x = 0 ; x < total_row ; x++ ){
//        for (int32_t y = 0; y < total_row ; y++){
//
//        }
//    }
}

int husky::pt::GetTypeByteSize(parquet::Type::type parquet_type) {
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
    return 0;
}

void husky::io::ParquetInputFormat::convertToRow() {
    auto total_row = getNumOfRow();
    auto total_column = getNumOfColumn();

    //clean up
    row_buffer = new Row[total_row]();

    int64_t values_read = 0;
    uint8_t** values = new uint8_t*[total_column];

    // loop all value and read to buffer
    for ( int x = 0; x < total_column; x++){
        size_t value_byte_size = pt::GetTypeByteSize(current_row_group_reader->Column(x)->descr()->physical_type());
        values[x] = new uint8_t[total_row * value_byte_size];
        parquet::ScanAllValues(total_row, nullptr, nullptr, values[x], &values_read, current_row_group_reader->Column(x).get());
    }

    for ( int x = 0; x < total_row; x++){
        Field* fields = new Field[total_column]();

        for ( int y = 0; y< total_column; y++){
            (fields + y)->set((void * )values[y][x]);
        }

        (row_buffer + x)->set(fields);
    }
}
