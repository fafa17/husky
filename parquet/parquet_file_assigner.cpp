//
// Created by Chris Liu on 8/11/2016.
//

#include <map>
#include "parquet_file_assigner.hpp"

int32_t parquet_file_batch_size = 100

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

void ParquetSplitAssinger::browse(const std::string* host, const std::string* port, const std::string &url) {
    //Recursive find all parquet file
    std::vector<hdfsFileInfo> fileList(parquet_file_batch_size);
    recursiveHdfsDirectoryList(fileList, HDFS::getHdfsFS(host, port), url, );

    //build all ParquetFileReader
    std::vector<ParquetFileReader> readers(parquet_file_batch_size);
    for(int x = 0 ; x < fileList.size ; x++){
        parquet::ParquetFileReader* reader = new parquet::ParquetFileReader()
        readers.psuh_back();
    }

    //read all SchemaDescriptors

    //build ParquetSplit , build vector
}
