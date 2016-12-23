//
// Created by Chris Liu on 22/12/2016.
//

#include <io/hdfs_manager.hpp>
#include "parquet/parquet_util.hpp"
#include "parquet/parquet_input_format.hpp"
#include "parquet/model/Row.hpp"
#include "core/engine.hpp"
#include <wchar.h>

class Record {
public:
    using KeyT = int;

    Record() = default;
    explicit Record(const KeyT& w) : key(w), a(false), b(0) {}
    explicit Record(const KeyT& w, bool& in_a, double& in_b) : key(w), a(in_a), b(in_b) {}
    const KeyT& id() const { return key; }

    KeyT key;
    bool a;
    double b;

    int count = 0;
};

void worker_procedure() {
    husky::io::ParquetInputFormat infmt;
    std::string input = husky::Context::get_param("input");
    infmt.set_input(input);

    auto& word_list = husky::ObjListFactory::create_objlist<Record>();
    auto& ch = husky::ChannelFactory::create_push_combined_channel<int, husky::SumCombiner<int>>(infmt, word_list);

    auto parse_wc = [&](Row& row) {
        //To get the index of column
        int ind_key = husky::parquet::getIndByColName(*infmt.getSchema(),"key");
        int ind_col_a = husky::parquet::getIndByColName(*infmt.getSchema(),"a");
        int ind_col_b = husky::parquet::getIndByColName(*infmt.getSchema(),"b");

        int key = row.get()->at(ind_key).getInt32();
        bool a_value = row.get()->at(ind_col_a).getBoolean();
        double b_value = row.get()->at(ind_col_b).getDouble();
        Record obj(key, a_value, b_value);
        word_list.add_object(obj);
    };

    husky::load(infmt, {&ch}, parse_wc);
    husky::list_execute(word_list, {&ch}, {}, [&ch](Record& word) {
        std::stringstream stream;
        stream << "DEBUG key:" << (int) word.key << " col1: "<< word.a << " col2: "<< word.b << std::endl;
        husky::base::log_msg(stream.str().c_str());
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