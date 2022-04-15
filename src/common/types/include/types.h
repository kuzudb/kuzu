#pragma once

#include <math.h>

#include <cassert>
#include <cstring>
#include <memory>
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

enum DataTypeID : uint8_t {
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

const string DataTypeIdNames[] = {"INVALID", "REL", "NODE", "LABEL", "BOOL", "INT64", "DOUBLE",
    "STRING", "NODE_ID", "UNSTRUCTURED", "DATE", "TIMESTAMP", "INTERVAL", "LIST"};

class DataType {
public:
    DataType() : typeID{INVALID}, childType{nullptr} {}
    explicit DataType(DataTypeID typeID) : typeID{typeID}, childType{nullptr} {
        assert(typeID != LIST);
    }
    DataType(DataTypeID typeID, unique_ptr<DataType> childType)
        : typeID{typeID}, childType{move(childType)} {
        assert(typeID == LIST);
    }

    DataType(const DataType& other);
    DataType(DataType&& other) noexcept : typeID{other.typeID}, childType{move(other.childType)} {}

    static inline vector<DataTypeID> getNumericalTypeIDs() {
        return vector<DataTypeID>{INT64, DOUBLE};
    }
    static inline vector<DataTypeID> getNumericalAndUnstructuredTypeIDs() {
        return vector<DataTypeID>{INT64, DOUBLE, UNSTRUCTURED};
    }

    DataType& operator=(const DataType& other);

    inline DataType& operator=(DataType&& other) noexcept {
        typeID = other.typeID;
        childType = move(other.childType);
        return *this;
    }

public:
    DataTypeID typeID;
    unique_ptr<DataType> childType;

private:
    unique_ptr<DataType> copy();
};

class Types {
public:
    static size_t getDataTypeSize(const DataType& dataType);
    static size_t getDataTypeSize(DataTypeID dataTypeID);
    static string dataTypeToString(const DataType& dataType);
    static string dataTypeToString(DataTypeID dataTypeID);
    static string dataTypesToString(const vector<DataType>& dataTypes);
    static string dataTypesToString(const vector<DataTypeID>& dataTypeIDs);
    static DataType dataTypeFromString(const string& dataTypeString);
    static bool isNumericalType(DataTypeID dataTypeID);
    static bool isLiteralType(DataTypeID dataTypeID);

private:
    static DataTypeID dataTypeIDFromString(const string& dataTypeIDString);
};

// RelDirection
enum RelDirection : uint8_t { FWD = 0, BWD = 1 };
const vector<RelDirection> REL_DIRECTIONS = {FWD, BWD};
RelDirection operator!(RelDirection& direction);

} // namespace common
} // namespace graphflow
