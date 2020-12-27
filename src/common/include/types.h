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

typedef uint32_t label_t;
typedef uint64_t node_offset_t;

const uint8_t TRUE = 1;
const uint8_t FALSE = 2;
const uint8_t NULL_BOOL = 0;
const int32_t NULL_INT = INT32_MIN;
const double_t NULL_DOUBLE = DBL_MIN;

//! Holds the cursor to reference a location in an in-mem page.
typedef struct PageCursor {
    uint64_t idx = -1;
    uint16_t offset = -1;
} PageCursor;

//! System representation for strings.
struct gf_string_t {
    uint32_t len;
    uint8_t prefix[4];
    union {
        uint8_t data[8];
        uint64_t overflowPtr;
    };

    inline void setOverflowPtrFromPageCursor(const PageCursor& cursor) {
        memcpy(&overflowPtr, &cursor.idx, 6);
        memcpy(((uint8_t*)&overflowPtr) + 6, &cursor.offset, 2);
    }

    inline void setOverflowPtrToPageCursor(PageCursor& cursor) {
        cursor.idx = 0;
        memcpy(&cursor.idx, &overflowPtr, 6);
        memcpy(&cursor.offset, ((uint8_t*)&overflowPtr) + 6, 2);
    }
};

//! System representation for nodeID.
struct nodeID_t {
    label_t label;
    node_offset_t offset;
};

enum DataType { REL, NODE, LABEL, BOOL, INT, DOUBLE, STRING };

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

// Rel Label Cardinality
enum Cardinality : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };

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
