#pragma once

#include <cstdint>
#include <string>

using namespace std;

class Literal {
public:
    Literal() = default;

    Literal(uint8_t value) { this->primitive.boolean = value; }

    Literal(int32_t value) { this->primitive.integer = value; }

    Literal(double value) { this->primitive.double_ = value; }

    Literal(const string& str) { this->str = str; }

    union PrimitiveValue {
        uint8_t boolean;
        int32_t integer;
        double double_;
    } primitive;

    string str;
};
