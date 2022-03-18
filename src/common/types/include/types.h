#pragma once

#include <math.h>

#include <cstring>
#include <string>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

typedef uint16_t sel_t;
typedef uint64_t hash_t;

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements;
    uint8_t* value;
};

enum DataType : uint8_t {
    INVALID = 0,

    REL = 1,
    NODE = 2,
    LABEL = 3,
    BOOL = 4,
    INT64 = 5,
    DOUBLE = 6,
    STRING = 7,
    NODE_ID = 8,
    UNSTRUCTURED = 9,
    DATE = 10,
    TIMESTAMP = 11,
    INTERVAL = 12,
    LIST = 13,
};

const string DataTypeNames[] = {"INVALID", "REL", "NODE", "LABEL", "BOOL", "INT64", "DOUBLE",
    "STRING", "NODE_ID", "UNSTRUCTURED", "DATE", "TIMESTAMP", "INTERVAL", "LIST"};

class Types {

public:
    static size_t getDataTypeSize(DataType dataType);
    static DataType getDataType(const std::string& dataTypeString);
    static string dataTypeToString(DataType dataType);
    static bool isNumericalType(DataType dataType);
};

// Direction
enum Direction : uint8_t { FWD = 0, BWD = 1 };

const vector<Direction> DIRECTIONS = {FWD, BWD};

Direction operator!(Direction& direction);

} // namespace common
} // namespace graphflow
