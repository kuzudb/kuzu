#pragma once

#include <cfloat>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

using namespace std;

namespace graphflow {
namespace common {

// System Types

typedef uint32_t gfLabel_t;
typedef uint64_t gfNodeOffset_t;
typedef int32_t gfInt_t;
typedef double_t gfDouble_t;
typedef uint8_t gfBool_t;
typedef string *gfString_t;

enum DataType : uint8_t { NODE, REL, INT, DOUBLE, BOOLEAN, STRING };

uint8_t getDataTypeSize(DataType dataType);

DataType getDataType(const string &dataTypeString);

const gfInt_t NULL_GFINT = INT32_MIN;
const gfDouble_t NULL_GFDOUBLE = DBL_MIN;
const gfString_t NULL_GFSTRING = nullptr;
const gfBool_t NULL_GFBOOL = 0;

// (Node/Rel) Property description struct

struct Property {

public:
    Property(){};

    Property(string propertyName, DataType dataType)
        : propertyName(propertyName), dataType(dataType){};

    bool operator==(const Property &o) const {
        return dataType == o.dataType && propertyName.compare(o.propertyName) == 0;
    }

public:
    string propertyName{};
    DataType dataType{};
};

struct hash_Property {
    size_t operator()(Property const &property) const {
        return (hash<string>{}(property.propertyName) * 31) + property.dataType;
    }
};

// Rel Label Cardinality

enum Cardinality : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };

Cardinality getCardinality(const string &cardinalityString);

// Direction

enum Direction : uint8_t { FORWARD = 0, BACKWARD = 1 };

const vector<Direction> DIRECTIONS = {FORWARD, BACKWARD};

Direction operator!(Direction &direction);

} // namespace common
} // namespace graphflow
