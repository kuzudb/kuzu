#pragma once

#include <cfloat>
#include <cmath>
#include <cstring>
#include <string>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

constexpr uint64_t DEFAULT_VECTOR_CAPACITY = 2048;
constexpr uint64_t NODE_SEQUENCE_VECTOR_CAPACITY = 1024;

typedef uint32_t label_t;
typedef uint64_t node_offset_t;

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

// Represents dates as days since 1970-01-01.
struct date_t {
    int32_t days;

    date_t() = default;
    explicit inline date_t(int32_t days_p) : days(days_p) {}
};

const uint8_t FALSE = 0;
const uint8_t TRUE = 1;

const uint8_t NULL_BOOL = 2;
const int32_t NULL_INT32 = INT32_MIN;
const double_t NULL_DOUBLE = DBL_MIN;
const date_t NULL_DATE = date_t(INT32_MIN);
const uint64_t NUM_BYTES_PER_NODE_ID = sizeof(node_offset_t) + sizeof(label_t);

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
    UNSTRUCTURED = 9,
    DATE = 10,
};

const string DataTypeNames[] = {"REL", "NODE", "LABEL", "BOOL", "INT32", "INT64", "DOUBLE",
    "STRING", "NODE_ID", "UNSTRUCTURED", "DATE"};

int32_t convertToInt32(char* data);

double_t convertToDouble(char* data);

uint8_t convertToBoolean(char* data);

size_t getDataTypeSize(DataType dataType);

DataType getDataType(const std::string& dataTypeString);

string dataTypeToString(DataType dataType);

bool isNumericalType(DataType dataType);

// Direction
enum Direction : uint8_t { FWD = 0, BWD = 1 };

const vector<Direction> DIRECTIONS = {FWD, BWD};

Direction operator!(Direction& direction);

} // namespace common
} // namespace graphflow
