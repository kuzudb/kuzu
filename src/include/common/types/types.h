#pragma once

#include <cassert>
#include <cmath>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/api.h"
#include "common/types/internal_id_t.h"

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
using property_id_t = uint32_t;
constexpr property_id_t INVALID_PROPERTY_ID = UINT32_MAX;
using column_id_t = property_id_t;
constexpr column_id_t INVALID_COLUMN_ID = INVALID_PROPERTY_ID;
using vector_idx_t = uint32_t;
constexpr vector_idx_t INVALID_VECTOR_IDX = UINT32_MAX;
using block_idx_t = uint64_t;
using field_idx_t = uint64_t;
using struct_field_idx_t = uint64_t;
using union_field_idx_t = uint64_t;
constexpr struct_field_idx_t INVALID_STRUCT_FIELD_IDX = UINT64_MAX;
using tuple_idx_t = uint64_t;
constexpr uint32_t UNDEFINED_CAST_COST = UINT32_MAX;

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements = 0;
    uint8_t* value = nullptr;
};

struct list_entry_t {
    common::offset_t offset;
    uint64_t size;

    list_entry_t() : offset{INVALID_OFFSET}, size{UINT64_MAX} {}
    list_entry_t(common::offset_t offset, uint64_t size) : offset{offset}, size{size} {}
};

struct struct_entry_t {
    int64_t pos;
};

using union_entry_t = struct_entry_t;

KUZU_API enum class LogicalTypeID : uint8_t {
    ANY = 0,
    NODE = 10,
    REL = 11,
    RECURSIVE_REL = 12,
    // SERIAL is a special data type that is used to represent a sequence of INT64 values that are
    // incremented by 1 starting from 0.
    SERIAL = 13,

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

    ARROW_COLUMN = 41,

    // variable size types
    STRING = 50,
    BLOB = 51,
    VAR_LIST = 52,
    STRUCT = 53,
    MAP = 54,
    UNION = 55,
};

enum class PhysicalTypeID : uint8_t {
    // Fixed size types.
    ANY = 0,
    BOOL = 1,
    INT64 = 2,
    INT32 = 3,
    INT16 = 4,
    DOUBLE = 5,
    FLOAT = 6,
    INTERVAL = 7,
    INTERNAL_ID = 9,
    ARROW_COLUMN = 10,

    // Variable size types.
    STRING = 20,
    FIXED_LIST = 21,
    VAR_LIST = 22,
    STRUCT = 23,
};

class LogicalType;

class ExtraTypeInfo {
public:
    virtual std::unique_ptr<ExtraTypeInfo> copy() const = 0;
    virtual ~ExtraTypeInfo() = default;
};

class VarListTypeInfo : public ExtraTypeInfo {
    friend class SerDeser;

public:
    VarListTypeInfo() = default;
    explicit VarListTypeInfo(std::unique_ptr<LogicalType> childType)
        : childType{std::move(childType)} {}
    inline LogicalType* getChildType() const { return childType.get(); }
    bool operator==(const VarListTypeInfo& other) const;
    std::unique_ptr<ExtraTypeInfo> copy() const override;

protected:
    std::unique_ptr<LogicalType> childType;
};

class FixedListTypeInfo : public VarListTypeInfo {
    friend class SerDeser;

public:
    FixedListTypeInfo() = default;
    explicit FixedListTypeInfo(
        std::unique_ptr<LogicalType> childType, uint64_t fixedNumElementsInList)
        : VarListTypeInfo{std::move(childType)}, fixedNumElementsInList{fixedNumElementsInList} {}
    inline uint64_t getNumElementsInList() const { return fixedNumElementsInList; }
    bool operator==(const FixedListTypeInfo& other) const;
    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    uint64_t fixedNumElementsInList;
};

class StructField {
    friend class SerDeser;

public:
    StructField() : type{std::make_unique<LogicalType>()} {}
    StructField(std::string name, std::unique_ptr<LogicalType> type);

    inline bool operator!=(const StructField& other) const { return !(*this == other); }
    inline std::string getName() const { return name; }
    inline LogicalType* getType() const { return type.get(); }

    bool operator==(const StructField& other) const;
    std::unique_ptr<StructField> copy() const;

private:
    std::string name;
    std::unique_ptr<LogicalType> type;
};

class StructTypeInfo : public ExtraTypeInfo {
    friend class SerDeser;

public:
    StructTypeInfo() = default;
    explicit StructTypeInfo(std::vector<std::unique_ptr<StructField>> fields);

    struct_field_idx_t getStructFieldIdx(std::string fieldName) const;
    StructField* getStructField(const std::string& fieldName) const;
    std::vector<LogicalType*> getChildrenTypes() const;
    std::vector<std::string> getChildrenNames() const;
    std::vector<StructField*> getStructFields() const;

    bool operator==(const kuzu::common::StructTypeInfo& other) const;

    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    std::vector<std::unique_ptr<StructField>> fields;
    std::unordered_map<std::string, struct_field_idx_t> fieldNameToIdxMap;
};

class LogicalType {
    friend class SerDeser;
    friend class LogicalTypeUtils;
    friend class StructType;
    friend class VarListType;
    friend class FixedListType;

public:
    KUZU_API LogicalType() : typeID{LogicalTypeID::ANY}, extraTypeInfo{nullptr} {};
    KUZU_API explicit LogicalType(LogicalTypeID typeID) : typeID{typeID}, extraTypeInfo{nullptr} {
        setPhysicalType();
    };
    KUZU_API LogicalType(LogicalTypeID typeID, std::unique_ptr<ExtraTypeInfo> extraTypeInfo)
        : typeID{typeID}, extraTypeInfo{std::move(extraTypeInfo)} {
        setPhysicalType();
    };
    KUZU_API LogicalType(const LogicalType& other);
    KUZU_API LogicalType(LogicalType&& other) noexcept;

    KUZU_API LogicalType& operator=(const LogicalType& other);

    KUZU_API bool operator==(const LogicalType& other) const;

    KUZU_API bool operator!=(const LogicalType& other) const;

    KUZU_API LogicalType& operator=(LogicalType&& other) noexcept;

    KUZU_API inline LogicalTypeID getLogicalTypeID() const { return typeID; }

    inline PhysicalTypeID getPhysicalType() const { return physicalType; }

    std::unique_ptr<LogicalType> copy();

private:
    void setPhysicalType();

private:
    LogicalTypeID typeID;
    PhysicalTypeID physicalType;
    std::unique_ptr<ExtraTypeInfo> extraTypeInfo;
};

struct VarListType {
    static inline LogicalType* getChildType(const LogicalType* type) {
        assert(type->getPhysicalType() == PhysicalTypeID::VAR_LIST);
        auto varListTypeInfo = reinterpret_cast<VarListTypeInfo*>(type->extraTypeInfo.get());
        return varListTypeInfo->getChildType();
    }
};

struct FixedListType {
    static inline LogicalType* getChildType(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(type->extraTypeInfo.get());
        return fixedListTypeInfo->getChildType();
    }

    static inline uint64_t getNumElementsInList(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(type->extraTypeInfo.get());
        return fixedListTypeInfo->getNumElementsInList();
    }
};

struct StructType {
    static inline std::vector<LogicalType*> getFieldTypes(const LogicalType* type) {
        assert(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getChildrenTypes();
    }

    static inline std::vector<std::string> getFieldNames(const LogicalType* type) {
        assert(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getChildrenNames();
    }

    static inline uint64_t getNumFields(const LogicalType* type) {
        assert(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        return getFieldTypes(type).size();
    }

    static inline std::vector<StructField*> getFields(const LogicalType* type) {
        assert(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructFields();
    }

    static inline StructField* getField(const LogicalType* type, const std::string& key) {
        assert(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructField(key);
    }

    static inline struct_field_idx_t getFieldIdx(const LogicalType* type, const std::string& key) {
        assert(type->getPhysicalType() == PhysicalTypeID::STRUCT);
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(type->extraTypeInfo.get());
        return structTypeInfo->getStructFieldIdx(key);
    }
};

struct MapType {
    static inline LogicalType* getKeyType(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::MAP);
        return StructType::getFieldTypes(VarListType::getChildType(type))[0];
    }

    static inline LogicalType* getValueType(const LogicalType* type) {
        assert(type->getLogicalTypeID() == LogicalTypeID::MAP);
        return StructType::getFieldTypes(VarListType::getChildType(type))[1];
    }
};

struct UnionType {
    static inline union_field_idx_t getInternalFieldIdx(union_field_idx_t idx) { return idx + 1; }

    static inline std::string getFieldName(const LogicalType* type, union_field_idx_t idx) {
        assert(type->getLogicalTypeID() == LogicalTypeID::UNION);
        return StructType::getFieldNames(type)[getInternalFieldIdx(idx)];
    }
};

struct PhysicalTypeUtils {
    static std::string physicalTypeToString(PhysicalTypeID physicalType);
    static uint32_t getFixedTypeSize(PhysicalTypeID physicalType);
};

class LogicalTypeUtils {
public:
    KUZU_API static std::string dataTypeToString(const LogicalType& dataType);
    KUZU_API static std::string dataTypeToString(LogicalTypeID dataTypeID);
    static std::string dataTypesToString(const std::vector<LogicalType>& dataTypes);
    static std::string dataTypesToString(const std::vector<LogicalTypeID>& dataTypeIDs);
    KUZU_API static LogicalType dataTypeFromString(const std::string& dataTypeString);
    static uint32_t getRowLayoutSize(const LogicalType& logicalType);
    static bool isNumerical(const LogicalType& dataType);
    static std::vector<LogicalType> getAllValidComparableLogicalTypes();
    static std::vector<LogicalTypeID> getNumericalLogicalTypeIDs();
    static std::vector<LogicalType> getAllValidLogicTypes();

private:
    static LogicalTypeID dataTypeIDFromString(const std::string& dataTypeIDString);
    static std::vector<std::string> parseStructFields(const std::string& structTypeStr);
};

enum class DBFileType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

} // namespace common
} // namespace kuzu
