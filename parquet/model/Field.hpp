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
    Field(std::__1::shared_ptr<void> obj);

    std::__1::shared_ptr<bool> getBoolean();
    std::__1::shared_ptr<uint8_t> getUint8();
    std::__1::shared_ptr<int8_t> getInt8();
    std::__1::shared_ptr<uint16_t> getUint16();
    std::__1::shared_ptr<int16_t> getInt16();
    std::__1::shared_ptr<uint32_t> getUint32();
    std::__1::shared_ptr<int32_t> getInt32();
    std::__1::shared_ptr<uint64_t> getUint64();
    std::__1::shared_ptr<int64_t> getInt64();
    std::__1::shared_ptr<float_t> getFloat();
    std::__1::shared_ptr<double_t> getDouble();
    std::__1::shared_ptr<std::__1::string> getString();

private :
    std::__1::shared_ptr<void> value;
};

#endif //PROJECT_FIELD_HPP
