//
// Reference:
// https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetInputFormat.java
// work like java getSplit

#include <vector>
#include <string>
#include <map>

#include "master/master.hpp"


enum SplitStatus { pending, reading, fail};

//Metadata for row group
struct RowGroupInfo{

    //Row group may consist of multi hdfs block
    std::vector<BlkDesc> hdfs_block_list;

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


private:

    int num_workers_alive;
    int num_split_per_job;
    std::map<int, std::vector<ParquetSplitInfo>> request_info_mapping;

};
