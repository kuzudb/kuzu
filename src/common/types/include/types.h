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
typedef uint32_t page_idx_t;
constexpr page_idx_t PAGE_IDX_MAX = UINT32_MAX;
typedef uint32_t list_header_t;

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements;
    uint8_t* value;
};

enum DataTypeID : uint8_t {
    // NOTE: Not all data types can be used in processor. For example, ANY should be resolved during
    // query compilation. Similarly logical data types should also only be used in compilation.
    // Some use cases are as follows.
    //    - differentiate whether is a variable refers to node table or rel table
    //    - bind (static evaluate) functions work on node/rel table.
    //      E.g. ID(a "datatype:NODE") -> node ID property "datatype:NODE_ID"

    // logical  types
    ANY = 0,
    NODE = 10,
    REL = 11,

    // physical fixed size types
    NODE_ID = 21,
    BOOL = 22,
    INT64 = 23,
    DOUBLE = 24,
    DATE = 25,
    TIMESTAMP = 26,
    INTERVAL = 27,

    STRING = 50,
    UNSTRUCTURED = 51,
    LIST = 52,
};

class DataType {
public:
    DataType() : typeID{ANY}, childType{nullptr} {}
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
    static inline vector<DataTypeID> getAllValidTypeIDs() {
        return vector<DataTypeID>{
            NODE_ID, BOOL, INT64, DOUBLE, STRING, UNSTRUCTURED, DATE, TIMESTAMP, INTERVAL, LIST};
    }

    DataType& operator=(const DataType& other);

    bool operator==(const DataType& other) const;

    bool operator!=(const DataType& other) const { return !((*this) == other); }

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
    static string dataTypeToString(const DataType& dataType);
    static string dataTypeToString(DataTypeID dataTypeID);
    static string dataTypesToString(const vector<DataType>& dataTypes);
    static string dataTypesToString(const vector<DataTypeID>& dataTypeIDs);
    static DataType dataTypeFromString(const string& dataTypeString);
    static const uint32_t getDataTypeSize(DataTypeID dataTypeID);
    static inline const uint32_t getDataTypeSize(const DataType& dataType) {
        return getDataTypeSize(dataType.typeID);
    }

private:
    static DataTypeID dataTypeIDFromString(const string& dataTypeIDString);
};

// RelDirection
enum RelDirection : uint8_t { FWD = 0, BWD = 1 };
const vector<RelDirection> REL_DIRECTIONS = {FWD, BWD};
RelDirection operator!(RelDirection& direction);

enum class DBFileType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

} // namespace common
} // namespace graphflow
