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

typedef uint32_t label_t;
typedef uint64_t node_offset_t;
typedef uint16_t sel_t;

// System representation for nodeID.
struct nodeID_t {
    node_offset_t offset;
    label_t label;

    nodeID_t() = default;
    explicit inline nodeID_t(node_offset_t _offset, label_t _label)
        : offset(_offset), label(_label) {}

    // comparison operators
    inline bool operator==(const nodeID_t& rhs) const {
        return offset == rhs.offset && label == rhs.label;
    };
    inline bool operator!=(const nodeID_t& rhs) const {
        return offset != rhs.offset || label != rhs.label;
    };
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

    // comparison operators
    inline bool operator==(const date_t& rhs) const { return days == rhs.days; };
    inline bool operator!=(const date_t& rhs) const { return days != rhs.days; };
    inline bool operator<=(const date_t& rhs) const { return days <= rhs.days; };
    inline bool operator<(const date_t& rhs) const { return days < rhs.days; };
    inline bool operator>(const date_t& rhs) const { return days > rhs.days; };
    inline bool operator>=(const date_t& rhs) const { return days >= rhs.days; };
};

const uint8_t FALSE = 0;
const uint8_t TRUE = 1;

const uint8_t NULL_BOOL = 2;
const int64_t NULL_INT64 = INT64_MIN;
const double_t NULL_DOUBLE = DBL_MIN;
const date_t NULL_DATE = date_t(INT32_MIN);
const uint64_t NUM_BYTES_PER_NODE_ID = sizeof(node_offset_t) + sizeof(label_t);

enum DataType : uint8_t {
    REL = 0,
    NODE = 1,
    LABEL = 2,
    BOOL = 3,
    INT64 = 4,
    DOUBLE = 5,
    STRING = 6,
    NODE_ID = 7,
    UNSTRUCTURED = 8,
    DATE = 9,
};

const string DataTypeNames[] = {
    "REL", "NODE", "LABEL", "BOOL", "INT64", "DOUBLE", "STRING", "NODE_ID", "UNSTRUCTURED", "DATE"};

// Direction
enum Direction : uint8_t { FWD = 0, BWD = 1 };

const vector<Direction> DIRECTIONS = {FWD, BWD};

Direction operator!(Direction& direction);

class TypeUtils {

public:
    static int64_t convertToInt64(const char* data);

    static double_t convertToDouble(const char* data);

    static uint8_t convertToBoolean(const char* data);

    static size_t getDataTypeSize(DataType dataType);

    static DataType getDataType(const std::string& dataTypeString);

    static string dataTypeToString(DataType dataType);

    static bool isNumericalType(DataType dataType);

    static string toString(uint8_t boolVal) {
        return boolVal == TRUE ? "True" : (boolVal == FALSE ? "False" : "");
    }

    static string toString(int64_t val) { return to_string(val); }

    static string toString(double val) { return to_string(val); }

    static string toString(nodeID_t val) {
        return to_string(val.label) + ":" + to_string(val.offset);
    }

private:
    static void throwConversionExceptionOutOfRange(const char* data, const DataType dataType);
    static void throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
        const char* data, const char* eptr, const DataType dataType);
    static string prefixConversionEexceptionMessage(const char* data, const DataType dataType);
};

} // namespace common
} // namespace graphflow
