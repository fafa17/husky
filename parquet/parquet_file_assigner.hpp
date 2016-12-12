//
// Reference:
// https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetInputFormat.java
// work like java getSplit

#include <vector>
#include <string>
#include <map>

#include "master/master.hpp"
#include "parquet/util"

class ParquetSplitAssinger {

public:
    ParquetSplitAssinger();

    ~ParquetSplitAssinger() = default;

    void master_main_handler();
    void master_setup_handler();
    void set_path(const std::string& url);
    std::pair<std::string, size_t> answer(std::string& host, std::string& url);


private:

    int num_workers_alive;
    int num_split_per_job;
    std::map<int, std::vector<ParquetSplitInfo>> request_info_mapping;

};
