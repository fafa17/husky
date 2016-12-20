//
// Created by Chris Liu on 19/12/2016.
//

#include "test.hpp"
#include "parquet_input_format.hpp"

int main(){
    husky::io::ParquetInputFormat format;
    format.setLocal("/Users/chrisliu/git/husky/resources/1col.parquet", 0, 100);
}
