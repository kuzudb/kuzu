#pragma once

#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstring>
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

class Property {

public:
    Property(){};

    Property(string name, DataType dataType) : name(name), dataType(dataType){};

public:
    string name{};
    DataType dataType{};
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

// c-like string equalTo.
struct charArrayEqualTo {
    bool operator()(const char* lhs, const char* rhs) const { return strcmp(lhs, rhs) == 0; }
};

// c-like string hasher.
struct charArrayHasher {
    size_t operator()(const char* key) const { return std::_Hash_bytes(key, strlen(key), 31); }
};

} // namespace common
} // namespace graphflow
