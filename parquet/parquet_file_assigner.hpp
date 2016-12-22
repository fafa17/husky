//
// Reference:
// https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetInputFormat.java
// work like java getSplit

#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <hdfs/hdfs.h>

#include "master/master.hpp"
#include "parquet/parquet_util.hpp"
#include "parquet/io/ParquetSplit.hpp"

namespace husky{

class ParquetSplitAssinger {

public:
    ParquetSplitAssinger();

    ~ParquetSplitAssinger() = default;

    void response();
    void setup();
    void init_hdfs(const std::string* host, const std::string* port);
    int browse(const std::string &url);
    ParquetSplit *answer(std::string &fileUrl);


private:

    int num_workers_alive;
    int num_split_per_job;

    //map url -> list of splits
    std::map<std::string, std::vector<ParquetSplit>> split_buffer_map;
    hdfsFS* current_fs;
};

void recursiveHdfsDirectoryList(std::vector<hdfsFileInfo>* fileList, hdfsFS fs, const std::string& url, const std::regex& fileFilter);
}