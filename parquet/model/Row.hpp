//
// Created by Chris Liu on 20/12/2016.
//

#ifndef PROJECT_ROW_HPP
#define PROJECT_ROW_HPP


#include "Field.hpp"

class Row{
public:
    Row(){}
    void set(Field* infields){
        fields = infields;
    }
    Field* get(){
        return fields;
    }
private:
    Field* fields;
};



#endif //PROJECT_ROW_HPP
