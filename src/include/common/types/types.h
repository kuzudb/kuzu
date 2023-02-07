#pragma once

#include <math.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/api.h"

namespace kuzu {
namespace common {

typedef uint16_t sel_t;
typedef uint64_t hash_t;
typedef uint32_t page_idx_t;
typedef uint32_t page_offset_t;
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

KUZU_API enum DataTypeID : uint8_t {
    // NOTE: Not all data types can be used in processor. For example, ANY should be resolved during
    // query compilation. Similarly logical data types should also only be used in compilation.
    // Some use cases are as follows.
    //    - differentiate whether is a variable refers to node table or rel table
    //    - bind (static evaluate) functions work on node/rel table.
    //      E.g. ID(a "datatype:NODE") -> node ID property "datatype:NODE_ID"

    // logical types

    ANY = 0,
    NODE = 10,
    REL = 11,

    // physical types

    // fixed size types
    BOOL = 22,
    INT64 = 23,
    DOUBLE = 24,
    DATE = 25,
    TIMESTAMP = 26,
    INTERVAL = 27,

    INTERNAL_ID = 40,

    // variable size types
    STRING = 50,
    LIST = 52,
};

class DataType {
public:
    KUZU_API DataType();
    KUZU_API explicit DataType(DataTypeID typeID);
    KUZU_API DataType(DataTypeID typeID, std::unique_ptr<DataType> childType);
    KUZU_API DataType(const DataType& other);
    KUZU_API DataType(DataType&& other) noexcept;

    static std::vector<DataTypeID> getNumericalTypeIDs();
    static std::vector<DataTypeID> getAllValidTypeIDs();

    KUZU_API DataType& operator=(const DataType& other);

    KUZU_API bool operator==(const DataType& other) const;

    KUZU_API bool operator!=(const DataType& other) const;

    KUZU_API DataType& operator=(DataType&& other) noexcept;

    KUZU_API DataTypeID getTypeID() const;
    KUZU_API DataType* getChildType() const;

public:
    DataTypeID typeID;
    std::unique_ptr<DataType> childType;

private:
    std::unique_ptr<DataType> copy();
};

class Types {
public:
    KUZU_API static std::string dataTypeToString(const DataType& dataType);
    KUZU_API static std::string dataTypeToString(DataTypeID dataTypeID);
    static std::string dataTypesToString(const std::vector<DataType>& dataTypes);
    static std::string dataTypesToString(const std::vector<DataTypeID>& dataTypeIDs);
    KUZU_API static DataType dataTypeFromString(const std::string& dataTypeString);
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
