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
    Field(){}
    ~Field(){
        delete value;
    }

    bool* getBoolean(){return (bool*)value;}
    uint8_t* getUint8(){return (uint8_t *)value;}
    int8_t* getInt8(){return (int8_t *)value;}
    uint16_t* getUint16(){return (uint16_t *)value;}
    int16_t* getInt16(){return (int16_t *)value;}
    uint32_t* getUint32(){return (uint32_t *)value;}
    int32_t* getInt32(){return (int32_t *)value;}
    uint64_t* getUint64(){return (uint64_t *)value;}
    int64_t* getInt64(){return (int64_t *)value;}
    float_t * getFloat(){return (float_t *)value;}
    double_t* getDouble(){return (double_t *)value;}
    std::string* getString(){return (std::string*)value;}

    void set(void* value){
        this->value = value;
    }

private :
    void* value;
};

#endif //PROJECT_FIELD_HPP
