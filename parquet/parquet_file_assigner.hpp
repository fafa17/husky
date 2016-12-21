//
// Reference:
// https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetInputFormat.java
// work like java getSplit

#include <vector>
#include <string>
#include <map>

#include "master/master.hpp"
#include "parquet/util"

struct ParquetSplit{
    std::string path;
    int32_t rowgroup_id;
};

class ParquetSplitAssinger {

public:
    ParquetSplitAssinger();

    ~ParquetSplitAssinger() = default;

    void master_main_handler();
    void master_setup_handler();
    void browse(const std::string& url);
    std::pair<std::string, size_t> answer(std::string& host, std::string& url);


private:

    int num_workers_alive;
    int num_split_per_job;

    std::vector<ParquetSplit>* split_buffer;
};
