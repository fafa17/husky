//
// Created by Chris Liu on 15/12/2016.
//

#include <iostream>
#include <regex>
#include "Field.hpp"

Field::Field(std::shared_ptr<void> obj) {
    this->value = obj;
}

std::shared_ptr<bool> Field::getBoolean(){
    return static_pointer_cast<bool>(value);
}

std::shared_ptr<uint8_t> Field::getUint8(){
    return static_pointer_cast<uint8_t>(value);
}

std::shared_ptr<int8_t> Field::getInt8(){
    return static_pointer_cast<int8_t>(value);
}

std::shared_ptr<uint16_t> Field::getUint16(){
    return static_pointer_cast<uint16_t>(value);
}

std::shared_ptr<int16_t> Field::getInt16(){
    return static_pointer_cast<int16_t>(value);
}

std::shared_ptr<uint32_t> Field::getUint32(){
    return static_pointer_cast<uint32_t>(value);
}

std::shared_ptr<int32_t> Field::getInt32(){
    return static_pointer_cast<int32_t>(value);
}

std::shared_ptr<uint64_t> Field::getUint64(){
    return static_pointer_cast<u_int64_t>(value);
}

std::shared_ptr<int64_t> Field::getInt64(){
    return static_pointer_cast<int64_t>(value);
}

std::shared_ptr<float_t> Field::getFloat(){
    return static_pointer_cast<float_t>(value);
}

std::shared_ptr<double_t> Field::getDouble(){
    return static_pointer_cast<double_t>(value);
}

std::shared_ptr<std::string> Field::getString() {
    return static_pointer_cast<std::string>(value);
}