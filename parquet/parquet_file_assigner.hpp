//
// Reference:
// https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetInputFormat.java
// work like java getSplit

#ifndef PROJECT_PARQUET_FILE_ASSIGNER_HPP
#define PROJECT_PARQUET_FILE_ASSIGNER_HPP

#include <vector>
#include <string>
#include <map>

enum SplitStatus { pending, reading, fail};

struct HDFSBlockInfo{
    int start_pos;
    int end_pos;
    std::string url;
};


//Metadata for row group
struct RowGroupInfo{

    //Row group may consist of multi hdfs block
    std::vector<HDFSBlockInfo> hdfs_block_list;

};


struct ParquetSplitInfo{

    std::vector<RowGroupInfo> row_group_list;
    SplitStatus status;

};


class ParquetSplitAssinger {

public:
    ParquetSplitAssinger();

    ParquetSplitAssinger(const std::map<int, std::vector<ParquetSplitInfo>> &request_info_mapping);

    ~ParquetSplitAssinger() = default;

    void master_main_handler();
    void master_setup_handler();
    void browse_local(const std::string& url);
    std::pair<std::string, size_t> answer(std::string& host, std::string& url);
    /// Return the number of workers who have finished reading the files in
    /// the given url
    int get_num_finished(std::string& url);
    /// Use this when all workers finish reading the files in url
    void finish_url(std::string& url);

private:

    int num_workers_alive;
    int num_split_per_job;
    std::map<int, std::vector<ParquetSplitInfo>> request_info_mapping;


};


#endif //PROJECT_PARQUET_FILE_ASSIGNER_HPP
