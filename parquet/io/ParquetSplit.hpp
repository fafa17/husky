//
// Created by LiuChris on 22/12/2016.
//

#ifndef PROJECT_PARQUETSPLIT_HPP
#define PROJECT_PARQUETSPLIT_HPP


class ParquetSplit{
    int32_t rowgroup_id;
    std::string path;

    std::string toString(){
        std::stringstream stream;
        stream << rowgroup_id;
        stream << path;
        return stream.str();
    }

    std::string fromString(std::string in_str){
        std::stringstream stream;
        stream.str(in_str);
        stream >> rowgroup_id;
        stream >> path;
        return stream.str();
    }
};

#endif //PROJECT_PARQUETSPLIT_HPP
