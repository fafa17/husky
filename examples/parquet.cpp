//
// Created by Chris Liu on 22/12/2016.
//

#include <io/hdfs_manager.hpp>
#include "parquet/parquet_input_format.hpp"
#include "parquet/model/Row.hpp"
#include "core/engine.hpp"

class Word {
public:
    using KeyT = int;

    Word() = default;
    explicit Word(const KeyT& w) : word(w) {}
    const KeyT& id() const { return word; }

    KeyT word;
    int count = 0;
};


void worker_procedure() {



    husky::io::ParquetInputFormat infmt;
    std::string input = husky::Context::get_param("input");
    infmt.set_input(input);

    auto& word_list = husky::ObjListFactory::create_objlist<Word>();
    auto& ch = husky::ChannelFactory::create_push_combined_channel<int, husky::SumCombiner<int>>(infmt, word_list);

    auto parse_wc = [&](Row& row) {
        Word obj(row.get()->getInt32());
        word_list.add_object(obj);
    };

    husky::load(infmt, {&ch}, parse_wc);
    husky::list_execute(word_list, {&ch}, {}, [&ch](Word& word) {
        husky::base::log_msg("DEBUG word:" + word.word);
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