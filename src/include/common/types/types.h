#pragma once

#include <math.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace kuzu {
namespace common {

typedef uint16_t sel_t;
typedef uint64_t hash_t;
typedef uint32_t page_idx_t;
constexpr page_idx_t PAGE_IDX_MAX = UINT32_MAX;
typedef uint32_t list_header_t;

typedef uint32_t property_id_t;
constexpr property_id_t INVALID_PROPERTY_ID = UINT32_MAX;

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements = 0;
    uint8_t* value = nullptr;
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
    LIST = 52,
};

class DataType {
public:
    DataType() : typeID{ANY}, childType{nullptr} {}
    explicit DataType(DataTypeID typeID) : typeID{typeID}, childType{nullptr} {
        assert(typeID != LIST);
    }
    DataType(DataTypeID typeID, std::unique_ptr<DataType> childType)
        : typeID{typeID}, childType{std::move(childType)} {
        assert(typeID == LIST);
    }

    DataType(const DataType& other);
    DataType(DataType&& other) noexcept
        : typeID{other.typeID}, childType{std::move(other.childType)} {}

    static inline std::vector<DataTypeID> getNumericalTypeIDs() {
        return std::vector<DataTypeID>{INT64, DOUBLE};
    }
    static inline std::vector<DataTypeID> getAllValidTypeIDs() {
        return std::vector<DataTypeID>{
            NODE_ID, BOOL, INT64, DOUBLE, STRING, DATE, TIMESTAMP, INTERVAL, LIST};
    }

    DataType& operator=(const DataType& other);

    bool operator==(const DataType& other) const;

    bool operator!=(const DataType& other) const { return !((*this) == other); }

    inline DataType& operator=(DataType&& other) noexcept {
        typeID = other.typeID;
        childType = std::move(other.childType);
        return *this;
    }

public:
    DataTypeID typeID;
    std::unique_ptr<DataType> childType;

private:
    std::unique_ptr<DataType> copy();
};

class Types {
public:
    static std::string dataTypeToString(const DataType& dataType);
    static std::string dataTypeToString(DataTypeID dataTypeID);
    static std::string dataTypesToString(const std::vector<DataType>& dataTypes);
    static std::string dataTypesToString(const std::vector<DataTypeID>& dataTypeIDs);
    static DataType dataTypeFromString(const std::string& dataTypeString);
    static uint32_t getDataTypeSize(DataTypeID dataTypeID);
    static inline uint32_t getDataTypeSize(const DataType& dataType) {
        return getDataTypeSize(dataType.typeID);
    }

private:
    static DataTypeID dataTypeIDFromString(const std::string& dataTypeIDString);
};

// RelDirection
enum RelDirection : uint8_t { FWD = 0, BWD = 1 };
const std::vector<RelDirection> REL_DIRECTIONS = {FWD, BWD};
RelDirection operator!(RelDirection& direction);
std::string getRelDirectionAsString(RelDirection relDirection);

enum class DBFileType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

} // namespace common
} // namespace kuzu
