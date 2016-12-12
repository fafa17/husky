//
// Created by Chris Liu on 8/11/2016.
//

#include <map>
#include "parquet_file_assigner.hpp"

ParquetSplitAssinger::ParquetSplitAssinger(const std::map<int, std::vector<ParquetSplitInfo>> &request_info_mapping)
        : request_info_mapping(request_info_mapping) {}

void ParquetSplitAssinger::set_path(const std::string &url) {

}
