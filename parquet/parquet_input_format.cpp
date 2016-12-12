//
// Created by Chris Liu on 8/11/2016.
//

#include "parquet_input_format.hpp"

#include "hdfs/hdfs.h"
#include "parquet/file/metadata.h"
#include "parquet/file/reader-internal.h"

#include <regex>
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

    parquet::FileMetaData::Make(&metadata_buffer[0], &footer_size);
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
