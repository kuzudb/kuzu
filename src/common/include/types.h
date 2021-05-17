#pragma once

#include <cfloat>
#include <cmath>
#include <cstring>
#include <string>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

constexpr uint64_t MAX_VECTOR_SIZE = 2048;
constexpr uint64_t NODE_SEQUENCE_VECTOR_SIZE = 1024;

typedef uint32_t label_t;
typedef uint64_t node_offset_t;

const uint8_t FALSE = 0;
const uint8_t TRUE = 1;

const uint8_t NULL_BOOL = 2;
const int32_t NULL_INT32 = INT32_MIN;
const double_t NULL_DOUBLE = DBL_MIN;
const uint64_t NUM_BYTES_PER_NODE_ID = sizeof(node_offset_t) + sizeof(label_t);

// System representation for nodeID.
struct nodeID_t {
    node_offset_t offset;
    label_t label;
};

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    uint64_t len;
    uint8_t* value;
};

enum DataType : uint8_t {
    REL = 0,
    NODE = 1,
    LABEL = 2,
    BOOL = 3,
    INT32 = 4,
    INT64 = 5,
    DOUBLE = 6,
    STRING = 7,
    NODE_ID = 8,
    UNSTRUCTURED = 9
};

const string DataTypeNames[] = {
    "REL", "NODE", "LABEL", "BOOL", "INT32", "INT64", "DOUBLE", "STRING", "NODE_ID", "UNKNOWN"};

int32_t convertToInt32(char* data);

double_t convertToDouble(char* data);

uint8_t convertToBoolean(char* data);

// A PropertyKey is a pair of index and a dataType. If the property is unstructured, then the
// dataType is UNKNOWN, otherwise it is one of those supported by the system.
class PropertyKey {

public:
    PropertyKey(){};

    PropertyKey(DataType dataType, uint32_t idx) : dataType{dataType}, idx{idx} {};

public:
    DataType dataType{};
    uint32_t idx{};
};

size_t getDataTypeSize(DataType dataType);

DataType getDataType(const std::string& dataTypeString);

string dataTypeToString(DataType dataType);

bool isNumericalType(DataType dataType);

// Rel Label Cardinality
enum Cardinality : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };
const string CardinalityNames[] = {"MANY_MANY", "MANY_ONE", "ONE_MANY", "ONE_ONE"};

Cardinality getCardinality(const string& cardinalityString);

// Direction
enum Direction : uint8_t { FWD = 0, BWD = 1 };

const vector<Direction> DIRS = {FWD, BWD};

Direction operator!(Direction& direction);

} // namespace common
} // namespace graphflow
