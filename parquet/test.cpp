//
// Created by Chris Liu on 19/12/2016.
//

#include <io/hdfs_manager.hpp>
#include <regex>
#include <iostream>
#include <core/engine.hpp>

#include "core/context.hpp"
#include "test.hpp"
#include "parquet_input_format.hpp"
//#include "master/parquet_file_assigner.hpp"

std::string parquet_url = "/parquet_1col";
const std::string host("127.0.0.1");
const std::string port("9000");
const std::string hdfsParquet("/parquet_1col/part-r-00000-6317221d-3fda-4789-96c4-ea22beb9899c.snappy.parquet");
//
//void testRecursiveHdfsDirectoryList(){
//    std::vector<hdfsFileInfo> fileList(0);
//    husky::io::HDFSManager::init(host, port);
//    auto fs = husky::io::HDFSManager::getInstance()->get_fs();
//    std::regex regex(".*.parquet");
//    husky::recursiveHdfsDirectoryList(&fileList, fs, parquet_url,regex);
//    assert(fileList.size() == 4);
//}
//
//
//void testbrowse(){
//    husky::ParquetSplitAssinger assinger;
//    husky::io::HDFSManager::init(host, port);
//    assinger.browse(parquet_url);
//}

void testHDFSRead(){
    husky::io::ParquetInputFormat format;
    husky::io::HDFSManager::init(host, port);
    auto fs = husky::io::HDFSManager::getInstance()->get_fs();
    format.setRowGroup(fs, hdfsParquet, 0);
    std::cout << format.getSchema()->name();
};

void testParquetInputFormat(){
    husky::io::HDFSManager::init(host, port);
    husky::io::ParquetInputFormat format;
    format.set_input(parquet_url);
    Row row;
    do{
        if(!format.next(row)){
            break;
        }
        std::cout << row.get()->at(0).getInt32() << std::endl;
    }while(true);
}
//int main(){
//    testParquetInputFormat();
//}