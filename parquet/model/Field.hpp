//
// Created by Chris Liu on 15/12/2016.
//

#ifndef PROJECT_FIELD_HPP
#define PROJECT_FIELD_HPP

#include "io/input/file_inputformat_impl.hpp"
#include "parquet/file/metadata.h"
#include "parquet/file/reader.h"

class Field{
public :
    Field();

    std::shared_ptr<bool> getBoolean();
    std::shared_ptr<uint8_t> getUint8();
    std::shared_ptr<int8_t> getInt8();
    std::shared_ptr<uint16_t> getUint16();
    std::shared_ptr<int16_t> getInt16();
    std::shared_ptr<uint32_t> getUint32();
    std::shared_ptr<int32_t> getInt32();
    std::shared_ptr<uint64_t> getUint64();
    std::shared_ptr<int64_t> getInt64();
    std::shared_ptr<float_t> getFloat();
    std::shared_ptr<double_t> getDouble();
    std::shared_ptr<std::string> getString();

    void set(void* value){
        this->value.reset(value);
    }

private :
    std::shared_ptr<void> value;
};

#endif //PROJECT_FIELD_HPP
