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

typedef uint64_t label_t;
typedef uint64_t node_offset_t;
typedef uint16_t sel_t;
typedef uint64_t hash_t;

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
    inline bool operator>(const nodeID_t& rhs) const {
        return (label > rhs.label) || (label == rhs.label && offset > rhs.offset);
    };
    inline bool operator>=(const nodeID_t& rhs) const {
        return (label > rhs.label) || (label == rhs.label && offset >= rhs.offset);
    };
    inline bool operator<(const nodeID_t& rhs) const {
        return (label < rhs.label) || (label == rhs.label && offset < rhs.offset);
    };
    inline bool operator<=(const nodeID_t& rhs) const {
        return (label < rhs.label) || (label == rhs.label && offset <= rhs.offset);
    };
};

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements;
    uint8_t* value;
};

struct interval_t {
    int32_t months;
    int32_t days;
    int64_t micros;

    interval_t() = default;
    explicit inline interval_t(int32_t months_p, int32_t days_p, int64_t micros_p)
        : months(months_p), days(days_p), micros(micros_p) {}

    // comparator operators
    inline bool operator==(const interval_t& rhs) const {
        return this->days == rhs.days && this->months == rhs.months && this->micros == rhs.micros;
    };
    inline bool operator!=(const interval_t& rhs) const { return !(*this == rhs); }

    bool operator>(const interval_t& rhs) const;
    inline bool operator<=(const interval_t& rhs) const { return !(*this > rhs); }
    inline bool operator<(const interval_t& rhs) const { return !(*this >= rhs); }
    inline bool operator>=(const interval_t& rhs) const { return *this > rhs || *this == rhs; }

    // arithmetic operators
    interval_t operator+(const interval_t& rhs) const;
    interval_t operator-(const interval_t& rhs) const;

    interval_t operator/(const uint64_t& rhs) const;
};

struct timestamp_t;

// System representation of dates as the number of days since 1970-01-01.
struct date_t {
    int32_t days;

    date_t() = default;
    explicit inline date_t(int32_t days_p) : days(days_p) {}

    // Comparison operators with date_t.
    inline bool operator==(const date_t& rhs) const { return days == rhs.days; };
    inline bool operator!=(const date_t& rhs) const { return days != rhs.days; };
    inline bool operator<=(const date_t& rhs) const { return days <= rhs.days; };
    inline bool operator<(const date_t& rhs) const { return days < rhs.days; };
    inline bool operator>(const date_t& rhs) const { return days > rhs.days; };
    inline bool operator>=(const date_t& rhs) const { return days >= rhs.days; };

    // Comparison operators with timestamp_t.
    bool operator==(const timestamp_t& rhs) const;
    inline bool operator!=(const timestamp_t& rhs) const { return !(*this == rhs); };
    bool operator<(const timestamp_t& rhs) const;
    inline bool operator<=(const timestamp_t& rhs) const {
        return (*this) < rhs || (*this) == rhs;
    };
    inline bool operator>(const timestamp_t& rhs) const { return !(*this <= rhs); };
    inline bool operator>=(const timestamp_t& rhs) const { return !(*this < rhs); };

    // arithmetic operators
    inline date_t operator+(const int32_t& day) const { return date_t(this->days + day); };
    inline date_t operator-(const int32_t& day) const { return date_t(this->days - day); };

    date_t operator+(const interval_t& interval) const;
    date_t operator-(const interval_t& interval) const;

    int64_t operator-(const date_t& rhs) const;
};

// Type used to represent time (microseconds)
struct dtime_t {
    int64_t micros;

    dtime_t() = default;
    explicit inline dtime_t(int64_t micros_p) : micros(micros_p) {}
    inline dtime_t& operator=(int64_t micros_p) {
        micros = micros_p;
        return *this;
    }

    // explicit conversion
    explicit inline operator int64_t() const { return micros; }
    explicit inline operator double() const { return micros; }

    // comparison operators
    inline bool operator==(const dtime_t& rhs) const { return micros == rhs.micros; };
    inline bool operator!=(const dtime_t& rhs) const { return micros != rhs.micros; };
    inline bool operator<=(const dtime_t& rhs) const { return micros <= rhs.micros; };
    inline bool operator<(const dtime_t& rhs) const { return micros < rhs.micros; };
    inline bool operator>(const dtime_t& rhs) const { return micros > rhs.micros; };
    inline bool operator>=(const dtime_t& rhs) const { return micros >= rhs.micros; };
};

// Type used to represent timestamps (value is in microseconds since 1970-01-01)
struct timestamp_t {
    int64_t value;

    timestamp_t() = default;
    explicit inline timestamp_t(int64_t value_p) : value(value_p) {}
    inline timestamp_t& operator=(int64_t value_p) {
        value = value_p;
        return *this;
    }

    // explicit conversion
    explicit inline operator int64_t() const { return value; }

    // Comparison operators with timestamp_t.
    inline bool operator==(const timestamp_t& rhs) const { return value == rhs.value; };
    inline bool operator!=(const timestamp_t& rhs) const { return value != rhs.value; };
    inline bool operator<=(const timestamp_t& rhs) const { return value <= rhs.value; };
    inline bool operator<(const timestamp_t& rhs) const { return value < rhs.value; };
    inline bool operator>(const timestamp_t& rhs) const { return value > rhs.value; };
    inline bool operator>=(const timestamp_t& rhs) const { return value >= rhs.value; };

    // Comparison operators with date_t.
    inline bool operator==(const date_t& rhs) const { return rhs == *this; };
    inline bool operator!=(const date_t& rhs) const { return !(rhs == *this); };
    inline bool operator<(const date_t& rhs) const { return rhs > *this; };
    inline bool operator<=(const date_t& rhs) const { return rhs >= *this; };
    inline bool operator>(const date_t& rhs) const { return rhs < *this; };
    inline bool operator>=(const date_t& rhs) const { return rhs <= *this; };

    // arithmetic operator
    timestamp_t operator+(const interval_t& interval) const;
    timestamp_t operator-(const interval_t& interval) const;

    interval_t operator-(const timestamp_t& rhs) const;
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

// Direction
enum Direction : uint8_t { FWD = 0, BWD = 1 };

const vector<Direction> DIRECTIONS = {FWD, BWD};

Direction operator!(Direction& direction);

class TypeUtils {

public:
    static int64_t convertToInt64(const char* data);

    static double_t convertToDouble(const char* data);

    static bool convertToBoolean(const char* data);

    static size_t getDataTypeSize(DataType dataType);

    static DataType getDataType(const std::string& dataTypeString);

    static string dataTypeToString(DataType dataType);

    static bool isNumericalType(DataType dataType);

    static string toString(bool boolVal) { return boolVal ? "True" : "False"; }

    static string toString(int64_t val) { return to_string(val); }

    static string toString(double val) { return to_string(val); }

    static string toString(nodeID_t val) {
        return to_string(val.label) + ":" + to_string(val.offset);
    }

private:
    static void throwConversionExceptionOutOfRange(const char* data, DataType dataType);
    static void throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
        const char* data, const char* eptr, DataType dataType);
    static string prefixConversionExceptionMessage(const char* data, DataType dataType);
};

} // namespace common
} // namespace graphflow
