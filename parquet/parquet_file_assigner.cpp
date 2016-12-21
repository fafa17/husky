//
// Created by Chris Liu on 8/11/2016.
//

#include <parquet/file/reader.h>
#include <regex>
#include <hdfs/hdfs.h>

#include "parquet_file_assigner.hpp"
#include "io/hdfs_manager.hpp"
#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "master/master.hpp"

namespace husky{

    //Helper function to get all the parquet files given a directory url
    //TODO: handle single file case
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

    static ParquetSplitAssinger parquetSplitAssinger;

    ParquetSplitAssinger::ParquetSplitAssinger() {
        Master::get_instance().register_main_handler(TYPE_PARQUET_BLK_REQ, std::bind(&ParquetSplitAssinger::response, this));
        Master::get_instance().register_setup_handler(std::bind(&ParquetSplitAssinger::setup, this));
    }

    //TODO: Error handling
    // 0: new read request
    // 1: already read
    // 2: can't find file
    int ParquetSplitAssinger::browse(const std::string &url) {

        //check the file read or not
        if(split_buffer_map.find(url) != split_buffer_map.end()){
            return 1;
        }
        auto hdfsfs = husky::io::HDFSManager::getInstance()->get_fs();

        //Recursive find all parquet file
        std::vector<hdfsFileInfo> fileList(0);
        std::regex parquet_post_regex(".*\\.parquet");
        recursiveHdfsDirectoryList(&fileList, hdfsfs, url, parquet_post_regex);

        //build all ParquetFileReader
        //read all SchemaDescriptors
        // build ParquetSplit , build vector
        //close all Reader
        std::vector<ParquetSplit>* splits = new std::vector<ParquetSplit>(0);
        for(int x = 0 ; x < fileList.size() ; x++){
            std::string hdfsFilePath(fileList[x].mName);
            auto reader = husky::parquet::getHDFSParquetFileReader(&hdfsfs, &hdfsFilePath);
            reader->RowGroup(0);
            auto groups = reader->metadata()->num_row_groups();
            for (int y = 0; y < groups ; y++){
                ParquetSplit* split = new ParquetSplit();
                split->path = hdfsFilePath;
                split->rowgroup_id = y;
                splits->push_back(*split);
            }
            reader->Close();
        }
        split_buffer_map.insert(*new std::pair<std::string, std::vector<ParquetSplit>>(url, *splits));
        return 0;
    }

    //Request: [host, fileurl]
    //Response: ParquetSplit serialize <-- if rowgroup=-1 all things is processed
    void ParquetSplitAssinger::response() {
        auto& master = Master::get_instance();
        auto socket = master.get_socket();
        base::BinStream request = zmq_recv_binstream(socket.get());
        std::string host = base::deser<std::string>(request);
        std::string fileurl = base::deser<std::string>(request);
        ParquetSplit* split = answer(fileurl);
        if(split == nullptr){
            split = new ParquetSplit();
            split->rowgroup_id = -1;
        }
        base::BinStream response;
        response << split->toString();
        zmq_sendmore_string(socket.get(), master.get_cur_client());
        zmq_sendmore_dummy(socket.get());
        zmq_send_binstream(socket.get(), response);
    }

    ParquetSplit* ParquetSplitAssinger::answer(std::string &fileUrl) {
        //check own map if the parquet file is opened or not
        //if not opend open it
        std::vector<ParquetSplit>* splits;
        if(split_buffer_map.find(fileUrl) == split_buffer_map.end()){
            browse(fileUrl);
        }
        splits = &split_buffer_map[fileUrl];

        //pick one splites and remove form the vector
        //TODO: check thread safe issue (critical)
        //TODO: Better error handling to prevent blk failure
        if( splits->size() > 0) {
            auto split = splits->back();
            splits->pop_back();
            return &split;
        }else {
            return nullptr;
        }
    }

    void ParquetSplitAssinger::setup() {
        husky::io::HDFSManager::init(Context::get_param("hdfs_namenode"), Context::get_param("hdfs_namenode_port"));
        num_workers_alive = Context::get_worker_info()->get_num_workers();
    }

}
