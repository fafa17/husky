//
// Created by Chris Liu on 22/12/2016.
//

#include <io/hdfs_manager.hpp>
#include "parquet/parquet_input_format.hpp"
#include "parquet/model/Row.hpp"
#include "core/engine.hpp"

class Record {
public:
    using KeyT = int;


    Record() = default;
    explicit Record(const KeyT& w) : key(w) {}
    const KeyT& id() const { return key; }

    KeyT key;
    int count = 0;
};

void worker_procedure() {
    husky::io::ParquetInputFormat infmt;
    std::string input = husky::Context::get_param("input");
    infmt.set_input(input);

    auto& word_list = husky::ObjListFactory::create_objlist<Record>();
    auto& ch = husky::ChannelFactory::create_push_combined_channel<int, husky::SumCombiner<int>>(infmt, word_list);

    auto parse_wc = [&](Row& row) {
        int value = row.get()->at(0).getInt32();
        Record obj(value);
        std::cout << obj.key << std::endl;
        word_list.add_object(obj);
    };

    husky::load(infmt, {&ch}, parse_wc);
    husky::list_execute(word_list, {&ch}, {}, [&ch](Record& word) {
        std::stringstream stream;
        stream << "DEBUG word:" << (int) word.key << std::endl;
        std::cout << stream.str();
    });
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");

    if (husky::init_with_args(argc, argv, args)) {

        husky::io::HDFSManager::init(husky::Context::get_param("hdfs_namenode"), husky::Context::get_param("hdfs_namenode_port"));

        husky::run_job(worker_procedure);
        return 0;
    }
    return 1;
}