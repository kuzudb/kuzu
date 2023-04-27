#pragma once

#include <cassert>
#include <cmath>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/api.h"
#include "common/string_utils.h"

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
using column_id_t = property_id_t;
constexpr column_id_t INVALID_COLUMN_ID = INVALID_PROPERTY_ID;
using vector_idx_t = uint32_t;
constexpr vector_idx_t INVALID_VECTOR_IDX = UINT32_MAX;
using block_idx_t = uint64_t;
using field_idx_t = uint64_t;

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
    // SERIAL is a special data type that is used to represent a sequence of INT64 values that are
    // incremented by 1 starting from 0.
    SERIAL = 12,

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
    STRUCT = 53,
};

class DataType;

class ExtraTypeInfo {
public:
    virtual std::unique_ptr<ExtraTypeInfo> copy() const = 0;
    virtual ~ExtraTypeInfo() = default;
};

class VarListTypeInfo : public ExtraTypeInfo {
    friend class SerDeser;

public:
    explicit VarListTypeInfo(std::unique_ptr<DataType> childType)
        : childType{std::move(childType)} {}
    VarListTypeInfo() = default;
    inline DataType* getChildType() const { return childType.get(); }
    bool operator==(const VarListTypeInfo& other) const;
    std::unique_ptr<ExtraTypeInfo> copy() const override;

protected:
    std::unique_ptr<DataType> childType;
};

class FixedListTypeInfo : public VarListTypeInfo {
    friend class SerDeser;

public:
    explicit FixedListTypeInfo(std::unique_ptr<DataType> childType, uint64_t fixedNumElementsInList)
        : VarListTypeInfo{std::move(childType)}, fixedNumElementsInList{fixedNumElementsInList} {}
    FixedListTypeInfo() = default;
    inline uint64_t getFixedNumElementsInList() const { return fixedNumElementsInList; }
    bool operator==(const FixedListTypeInfo& other) const;
    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    uint64_t fixedNumElementsInList;
};

class StructField {
    friend class SerDeser;

public:
    StructField() : type{std::make_unique<DataType>()} {}
    StructField(std::string name, std::unique_ptr<DataType> type)
        : name{std::move(name)}, type{std::move(type)} {
        // Note: struct field name is case-insensitive.
        StringUtils::toUpper(this->name);
    }
    inline bool operator!=(const StructField& other) const { return !(*this == other); }
    inline std::string getName() const { return name; }
    inline DataType* getType() const { return type.get(); }
    bool operator==(const StructField& other) const;
    std::unique_ptr<StructField> copy() const;

private:
    std::string name;
    std::unique_ptr<DataType> type;
};

class StructTypeInfo : public ExtraTypeInfo {
    friend class SerDeser;

public:
    StructTypeInfo() = default;
    explicit StructTypeInfo(std::vector<std::unique_ptr<StructField>> fields)
        : fields{std::move(fields)} {}

    std::vector<DataType*> getChildrenTypes() const;
    std::vector<std::string> getChildrenNames() const;
    std::vector<StructField*> getStructFields() const;

    bool operator==(const kuzu::common::StructTypeInfo& other) const;
    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    std::vector<std::unique_ptr<StructField>> fields;
};

class DataType {
public:
    KUZU_API DataType() : typeID{ANY}, extraTypeInfo{nullptr} {};
    KUZU_API explicit DataType(DataTypeID typeID) : typeID{typeID}, extraTypeInfo{nullptr} {};
    KUZU_API DataType(std::unique_ptr<DataType> childType)
        : typeID{VAR_LIST}, extraTypeInfo{std::make_unique<VarListTypeInfo>(std::move(childType))} {
    }
    KUZU_API DataType(std::unique_ptr<DataType> childType, uint64_t fixedNumElementsInList)
        : typeID{FIXED_LIST}, extraTypeInfo{std::make_unique<FixedListTypeInfo>(
                                  std::move(childType), fixedNumElementsInList)} {}
    KUZU_API DataType(std::vector<std::unique_ptr<StructField>> childrenTypes)
        : typeID{STRUCT}, extraTypeInfo{
                              std::make_unique<StructTypeInfo>(std::move(childrenTypes))} {};
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

    DataType* getChildType() const;

    std::unique_ptr<DataType> copy();

    ExtraTypeInfo* getExtraTypeInfo() const;

public:
    DataTypeID typeID;
    std::unique_ptr<ExtraTypeInfo> extraTypeInfo;
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
