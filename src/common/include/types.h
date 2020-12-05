#pragma once

#include <cfloat>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

//! Types used for vertex, edge, and label identification.
typedef uint32_t label_t;
typedef uint64_t node_offset_t;

const uint8_t TRUE = 1;
const uint8_t FALSE = 2;
const uint8_t NULL_BOOL = 0;
const int32_t NULL_INT = INT32_MIN;
const double_t NULL_DOUBLE = DBL_MIN;

struct node_t {
    label_t label;
    node_offset_t nodeOffset;
};

enum DataType { REL, NODE, LABEL, BOOL, INT, DOUBLE, STRING };

struct Property {

public:
    Property(){};

    Property(string name, DataType dataType) : name(name), dataType(dataType){};

    bool operator==(const Property& o) const {
        return dataType == o.dataType && name.compare(o.name) == 0;
    }

public:
    string name{};
    DataType dataType{};
};

struct hash_Property {
    size_t operator()(Property const& property) const {
        return (hash<string>{}(property.name) * 31) + property.dataType;
    }
};

uint8_t getDataTypeSize(DataType dataType);

DataType getDataType(const std::string& dataTypeString);

// Rel Label Cardinality
enum Cardinality : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };

Cardinality getCardinality(const string& cardinalityString);

// Direction
enum Direction : uint8_t { FWD = 0, BWD = 1 };

const vector<Direction> DIRS = {FWD, BWD};

Direction operator!(Direction& direction);

} // namespace common
} // namespace graphflow
