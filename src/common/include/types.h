#pragma once

#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "robin_hood.h"

using namespace std;

namespace graphflow {
namespace common {

constexpr uint32_t MAX_VECTOR_SIZE = 2048;
constexpr uint32_t NODE_SEQUENCE_VECTOR_SIZE = 1024;

typedef uint32_t label_t;
typedef uint64_t node_offset_t;

const uint8_t FALSE = 0;
const uint8_t TRUE = 1;

const uint8_t NULL_BOOL = 2;
const int32_t NULL_INT32 = INT32_MIN;
const double_t NULL_DOUBLE = DBL_MIN;
const uint64_t NUM_BYTES_PER_NODE_ID = sizeof(node_offset_t) + sizeof(label_t);

// Holds the cursor to reference a location in an in-mem page.
typedef struct PageCursor {
    uint64_t idx = -1;
    uint16_t offset = -1;
} PageCursor;

// System representation for strings.
struct gf_string_t {

    static const uint64_t PREFIX_LENGTH = sizeof(uint8_t) * 4;
    static const uint64_t STR_LENGTH_PLUS_PREFIX_LENGTH = sizeof(uint32_t) + PREFIX_LENGTH;
    static const uint64_t INLINED_SUFFIX_LENGTH = sizeof(uint8_t) * 8;
    static const uint64_t SHORT_STR_LENGTH = sizeof(uint8_t) * 12;

    uint32_t len;
    uint8_t prefix[4];
    union {
        uint8_t data[8];
        uint64_t overflowPtr;
    };

    void copyOverflowPtrFromPageCursor(const PageCursor& cursor);
    void copyOverflowPtrToPageCursor(PageCursor& cursor);

    inline bool isShort() const { return len <= SHORT_STR_LENGTH; }

    inline const uint8_t* getData() const {
        return len <= SHORT_STR_LENGTH ? data : reinterpret_cast<uint8_t*>(overflowPtr);
    }
};

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
    UNKNOWN = 8
};

const string DataTypeNames[] = {
    "REL", "NODE", "LABEL", "BOOL", "INT32", "INT64", "DOUBLE", "STRING", "UNKNOWN"};

int32_t convertToInt32(char* data);

double_t convertToDouble(char* data);

uint8_t convertToBoolean(char* data);

class Property {

public:
    Property(){};

    Property(DataType dataType, uint32_t idx) : dataType{dataType}, idx{idx} {};

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

// C-like string equalTo.
struct charArrayEqualTo {
    bool operator()(const char* lhs, const char* rhs) const { return strcmp(lhs, rhs) == 0; }
};

// C-like string hasher.
struct charArrayHasher {
    size_t operator()(const char* key) const { return robin_hood::hash_bytes(key, strlen(key)); }
};

} // namespace common
} // namespace graphflow
