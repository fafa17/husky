//
// Created by Chris Liu on 8/11/2016.
//

#include "parquet_input_format.hpp"

#include "hdfs/hdfs.h"
#include "parquet/file/metadata.h"

#include <regex>
#include <iostream>
#include <hdfs_lib/include/hdfs/hdfs.h>

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

/**
 * To get the size of the footer of a Parquet file
 * @param file
 * @param info
 * @param fs
 * @return
 */
int32_t getParquetFooterSize(hdfsFile file, hdfsFileInfo* info, hdfsFS fs){
    auto filesize = info->mSize;
    int64_t footer_size_start = filesize - _parquet_footer_size_len - _parquet_magic_len;

    //Footer Size from the hdfs block
    int32_t* footer_size;

    hdfsPread(fs, file, footer_size_start, footer_size, _parquet_footer_size_len);
}


parquet::FileMetaData* getParquetFooter(std::string& url, hdfsFS fs) {
    auto *file_info = hdfsGetPathInfo(fs, url.c_str());
    auto file_size = file_info->mSize;

    auto hdfs_file = hdfsOpenFile(fs, url.c_str(), O_RDONLY, 0, 0, 0);
    auto footer_size = getParquetFooterSize(hdfs_file, file_info, fs);

//    parquet::FileMetaData::Make(&metadata_buffer[0], &footer_size);
}

bool husky::io::ParquetLoader::load(const std::string& url) {
    //hadoop read
    //list all files
    std::regex parquetSuffixFilter("*.parquet");
    auto * file_list = new std::vector<hdfsFileInfo>();
    recursiveHdfsDirectoryList(file_list, this->hdfs, url, parquetSuffixFilter);

    //get footers of all files
    //TODO: checking schema consistency over files
    for ( int x = 0; x < file_list->size(); x++ ){

    }
    //Create new split object for each row group
    //set to assigner
    return false;
}

husky::io::Field::Field(std::shared_ptr<void> obj) {
    this->value = obj;
}
std::shared_ptr<bool> husky::io::Field::getBoolean(){
    return std::static_pointer_cast<bool>(value);
}
std::shared_ptr<uint8_t> husky::io::Field::getUint8(){
    return std::static_pointer_cast<uint8_t>(value);
}
std::shared_ptr<int8_t> husky::io::Field::getInt8(){
    return std::static_pointer_cast<int8_t>(value);
}
std::shared_ptr<uint16_t> husky::io::Field::getUint16(){
    return std::static_pointer_cast<uint16_t>(value);
}
std::shared_ptr<int16_t> husky::io::Field::getInt16(){
    return std::static_pointer_cast<int16_t>(value);
}
std::shared_ptr<uint32_t> husky::io::Field::getUint32(){
    return std::static_pointer_cast<uint32_t>(value);
}
std::shared_ptr<int32_t> husky::io::Field::getInt32(){
    return std::static_pointer_cast<int32_t>(value);
}
std::shared_ptr<uint64_t> husky::io::Field::getUint64(){
    return std::static_pointer_cast<u_int64_t>(value);
}
std::shared_ptr<int64_t> husky::io::Field::getInt64(){
    return std::static_pointer_cast<int64_t>(value);
}
std::shared_ptr<float_t> husky::io::Field::getFloat(){
    return std::static_pointer_cast<float_t>(value);
}
std::shared_ptr<double_t> husky::io::Field::getDouble(){
    return std::static_pointer_cast<double_t>(value);
}
std::shared_ptr<std::string> husky::io::Field::getString() {
    return std::static_pointer_cast<std::string>(value);
}

int main()
{
    auto field = husky::io::Field(std::shared_ptr<int32_t>(new int32_t(10)));

    std::cout << field.getInt32();
}

