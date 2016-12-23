//
// Created by Chris Liu on 15/12/2016.
//

#include <iostream>
#include <regex>
#include <locale.h>
#include <clocale>

#include "Field.hpp"

std::wstring Field::getString() {
    parquet::ByteArray ba = *reinterpret_cast<parquet::ByteArray*>(value);
    std::wstring text((wchar_t *)ba.ptr,ba.len);
    return text;
}
