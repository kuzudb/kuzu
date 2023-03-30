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

using sel_t = uint16_t;
using hash_t = uint64_t;
using page_idx_t = uint32_t;
using frame_idx_t = page_idx_t;
using page_offset_t = uint32_t;
constexpr page_idx_t INVALID_PAGE_IDX = UINT32_MAX;
using page_group_idx_t = uint32_t;
using frame_group_idx_t = page_group_idx_t;
using list_header_t = uint32_t;
using property_id_t = uint32_t;
constexpr property_id_t INVALID_PROPERTY_ID = UINT32_MAX;
using vector_idx_t = uint32_t;
constexpr vector_idx_t INVALID_VECTOR_IDX = UINT32_MAX;
using block_idx_t = uint64_t;

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
    INT32 = 24,
    INT16 = 25,
    DOUBLE = 26,
    FLOAT = 27,
    DATE = 28,
    TIMESTAMP = 29,
    INTERVAL = 30,
    FIXED_LIST = 31,

    INTERNAL_ID = 40,

    // variable size types
    STRING = 50,
    VAR_LIST = 52,
};

class DataType {
public:
    KUZU_API DataType();
    KUZU_API explicit DataType(DataTypeID typeID);
    KUZU_API DataType(DataTypeID typeID, std::unique_ptr<DataType> childType);
    KUZU_API DataType(
        DataTypeID typeID, std::unique_ptr<DataType> childType, uint64_t fixedNumElementsInList);
    KUZU_API DataType(const DataType& other);
    KUZU_API DataType(DataType&& other) noexcept;

    static std::vector<DataTypeID> getNumericalTypeIDs();
    static std::vector<DataTypeID> getAllValidComparableTypes();
    static std::vector<DataTypeID> getAllValidTypeIDs();

    KUZU_API DataType& operator=(const DataType& other);

    KUZU_API bool operator==(const DataType& other) const;

    KUZU_API bool operator!=(const DataType& other) const;

    KUZU_API DataType& operator=(DataType&& other) noexcept;

    KUZU_API DataTypeID getTypeID() const;
    KUZU_API DataType* getChildType() const;

public:
    DataTypeID typeID;
    // The following fields are only used by LIST DataType.
    std::unique_ptr<DataType> childType;
    uint64_t fixedNumElementsInList = UINT64_MAX;

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
    static uint32_t getDataTypeSize(const DataType& dataType);
    static bool isNumerical(const DataType& dataType);

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
