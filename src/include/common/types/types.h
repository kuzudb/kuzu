#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/api.h"
#include "common/cast.h"
#include "common/types/internal_id_t.h"
#include "common/types/interval_t.h"

namespace kuzu {
namespace processor {
class ParquetReader;
};
namespace common {

class Serializer;
class Deserializer;
struct FileInfo;

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
using idx_t = uint32_t;
constexpr vector_idx_t INVALID_VECTOR_IDX = UINT32_MAX;
using block_idx_t = uint64_t;
constexpr block_idx_t INVALID_BLOCK_IDX = UINT64_MAX;
using struct_field_idx_t = uint8_t;
using union_field_idx_t = struct_field_idx_t;
constexpr struct_field_idx_t INVALID_STRUCT_FIELD_IDX = UINT8_MAX;
using row_idx_t = uint64_t;
constexpr row_idx_t INVALID_ROW_IDX = UINT64_MAX;
constexpr uint32_t UNDEFINED_CAST_COST = UINT32_MAX;
using node_group_idx_t = uint64_t;
constexpr node_group_idx_t INVALID_NODE_GROUP_IDX = UINT64_MAX;
using partition_idx_t = uint64_t;
constexpr partition_idx_t INVALID_PARTITION_IDX = UINT64_MAX;
using length_t = uint64_t;
using list_size_t = uint32_t;
using sequence_id_t = uint64_t;

using transaction_t = uint64_t;
constexpr transaction_t INVALID_TRANSACTION = UINT64_MAX;

// System representation for a variable-sized overflow value.
struct overflow_value_t {
    // the size of the overflow buffer can be calculated as:
    // numElements * sizeof(Element) + nullMap(4 bytes alignment)
    uint64_t numElements = 0;
    uint8_t* value = nullptr;
};

struct list_entry_t {
    common::offset_t offset;
    list_size_t size;

    list_entry_t() : offset{INVALID_OFFSET}, size{UINT32_MAX} {}
    list_entry_t(common::offset_t offset, list_size_t size) : offset{offset}, size{size} {}
};

struct struct_entry_t {
    int64_t pos;
};

struct map_entry_t {
    list_entry_t entry;
};

struct union_entry_t {
    struct_entry_t entry;
};

struct int128_t;
struct ku_string_t;

template<typename T>
concept IntegerTypes =
    std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
    std::is_same_v<T, int64_t> || std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
    std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t> ||
    std::is_same_v<T, common::int128_t>;

template<typename T>
concept NumericTypes = IntegerTypes<T> || std::floating_point<T>;

template<typename T>
concept ComparableTypes = NumericTypes<T> || std::is_same_v<T, common::ku_string_t> ||
                          std::is_same_v<T, interval_t> || std::is_same_v<T, bool>;

template<typename T>
concept HashablePrimitive = ((std::integral<T> && !std::is_same_v<T, bool>) ||
                             std::floating_point<T> || std::is_same_v<T, common::int128_t>);
template<typename T>
concept IndexHashable =
    ((std::integral<T> && !std::is_same_v<T, bool>) || std::floating_point<T> ||
        std::is_same_v<T, common::int128_t> || std::is_same_v<T, common::ku_string_t> ||
        std::is_same_v<T, std::string_view> || std::same_as<T, std::string>);

template<typename T>
concept HashableTypes = (std::integral<T> || std::floating_point<T> ||
                         std::is_same_v<T, common::int128_t> || std::is_same_v<T, struct_entry_t> ||
                         std::is_same_v<T, list_entry_t> || std::is_same_v<T, internalID_t> ||
                         std::is_same_v<T, interval_t> || std::is_same_v<T, ku_string_t>);

enum class LogicalTypeID : uint8_t {
    ANY = 0,
    NODE = 10,
    REL = 11,
    RECURSIVE_REL = 12,
    // SERIAL is a special data type that is used to represent a sequence of INT64 values that are
    // incremented by 1 starting from 0.
    SERIAL = 13,

    BOOL = 22,
    INT64 = 23,
    INT32 = 24,
    INT16 = 25,
    INT8 = 26,
    UINT64 = 27,
    UINT32 = 28,
    UINT16 = 29,
    UINT8 = 30,
    INT128 = 31,
    DOUBLE = 32,
    FLOAT = 33,
    DATE = 34,
    TIMESTAMP = 35,
    TIMESTAMP_SEC = 36,
    TIMESTAMP_MS = 37,
    TIMESTAMP_NS = 38,
    TIMESTAMP_TZ = 39,
    INTERVAL = 40,

    INTERNAL_ID = 42,

    STRING = 50,
    BLOB = 51,

    LIST = 52,
    ARRAY = 53,
    STRUCT = 54,
    MAP = 55,
    UNION = 56,
    RDF_VARIANT = 57,
    POINTER = 58,

    UUID = 59
};

enum class PhysicalTypeID : uint8_t {
    // Fixed size types.
    ANY = 0,
    BOOL = 1,
    INT64 = 2,
    INT32 = 3,
    INT16 = 4,
    INT8 = 5,
    UINT64 = 6,
    UINT32 = 7,
    UINT16 = 8,
    UINT8 = 9,
    INT128 = 10,
    DOUBLE = 11,
    FLOAT = 12,
    INTERVAL = 13,
    INTERNAL_ID = 14,

    // Variable size types.
    STRING = 20,
    LIST = 22,
    ARRAY = 23,
    STRUCT = 24,
    POINTER = 25,
};

class ExtraTypeInfo;
class StructField;
class StructTypeInfo;

class LogicalType {
    friend struct LogicalTypeUtils;
    friend struct StructType;
    friend struct ListType;
    friend struct ArrayType;

public:
    KUZU_API LogicalType() : typeID{LogicalTypeID::ANY}, extraTypeInfo{nullptr} {
        physicalType = getPhysicalType(this->typeID);
    };
    explicit KUZU_API LogicalType(LogicalTypeID typeID);
    KUZU_API LogicalType(const LogicalType& other);
    KUZU_API LogicalType(LogicalType&& other) = default;

    KUZU_API LogicalType& operator=(const LogicalType& other);
    KUZU_API bool operator==(const LogicalType& other) const;
    KUZU_API bool operator!=(const LogicalType& other) const;
    KUZU_API LogicalType& operator=(LogicalType&& other) = default;

    KUZU_API std::string toString() const;
    static LogicalType fromString(const std::string& str);

    KUZU_API LogicalTypeID getLogicalTypeID() const { return typeID; }
    bool containsAny() const;

    PhysicalTypeID getPhysicalType() const { return physicalType; }
    static PhysicalTypeID getPhysicalType(LogicalTypeID logicalType);

    void setExtraTypeInfo(std::unique_ptr<ExtraTypeInfo> typeInfo) {
        extraTypeInfo = std::move(typeInfo);
    }

    void serialize(Serializer& serializer) const;

    static std::unique_ptr<LogicalType> deserialize(Deserializer& deserializer);

    KUZU_API std::unique_ptr<LogicalType> copy() const;

    static std::vector<std::unique_ptr<LogicalType>> copy(
        const std::vector<std::unique_ptr<LogicalType>>& types);
    static std::vector<LogicalType> copy(const std::vector<LogicalType>& types);
    static std::vector<LogicalType> copy(const std::vector<LogicalType*>& types);

    static std::unique_ptr<LogicalType> ANY() {
        return std::make_unique<LogicalType>(LogicalTypeID::ANY);
    }
    static std::unique_ptr<LogicalType> BOOL() {
        return std::make_unique<LogicalType>(LogicalTypeID::BOOL);
    }
    static std::unique_ptr<LogicalType> HASH() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT64);
    }
    static std::unique_ptr<LogicalType> INT64() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT64);
    }
    static std::unique_ptr<LogicalType> INT32() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT32);
    }
    static std::unique_ptr<LogicalType> INT16() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT16);
    }
    static std::unique_ptr<LogicalType> INT8() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT8);
    }
    static std::unique_ptr<LogicalType> UINT64() {
        return std::make_unique<LogicalType>(LogicalTypeID::UINT64);
    }
    static std::unique_ptr<LogicalType> UINT32() {
        return std::make_unique<LogicalType>(LogicalTypeID::UINT32);
    }
    static std::unique_ptr<LogicalType> UINT16() {
        return std::make_unique<LogicalType>(LogicalTypeID::UINT16);
    }
    static std::unique_ptr<LogicalType> UINT8() {
        return std::make_unique<LogicalType>(LogicalTypeID::UINT8);
    }
    static std::unique_ptr<LogicalType> INT128() {
        return std::make_unique<LogicalType>(LogicalTypeID::INT128);
    }
    static std::unique_ptr<LogicalType> DOUBLE() {
        return std::make_unique<LogicalType>(LogicalTypeID::DOUBLE);
    }
    static std::unique_ptr<LogicalType> FLOAT() {
        return std::make_unique<LogicalType>(LogicalTypeID::FLOAT);
    }
    static std::unique_ptr<LogicalType> DATE() {
        return std::make_unique<LogicalType>(LogicalTypeID::DATE);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP_NS() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP_NS);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP_MS() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP_MS);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP_SEC() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP_SEC);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP_TZ() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP_TZ);
    }
    static std::unique_ptr<LogicalType> TIMESTAMP() {
        return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP);
    }
    static std::unique_ptr<LogicalType> INTERVAL() {
        return std::make_unique<LogicalType>(LogicalTypeID::INTERVAL);
    }
    static std::unique_ptr<LogicalType> INTERNAL_ID() {
        return std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID);
    }
    static std::unique_ptr<LogicalType> SERIAL() {
        return std::make_unique<LogicalType>(LogicalTypeID::SERIAL);
    }
    static std::unique_ptr<LogicalType> STRING() {
        return std::make_unique<LogicalType>(LogicalTypeID::STRING);
    }
    static std::unique_ptr<LogicalType> BLOB() {
        return std::make_unique<LogicalType>(LogicalTypeID::BLOB);
    }
    static std::unique_ptr<LogicalType> UUID() {
        return std::make_unique<LogicalType>(LogicalTypeID::UUID);
    }
    static std::unique_ptr<LogicalType> POINTER() {
        return std::make_unique<LogicalType>(LogicalTypeID::POINTER);
    }
    static KUZU_API std::unique_ptr<LogicalType> STRUCT(std::vector<StructField>&& fields);

    static KUZU_API std::unique_ptr<LogicalType> RECURSIVE_REL(
        std::unique_ptr<StructTypeInfo> typeInfo);

    static KUZU_API std::unique_ptr<LogicalType> NODE(std::unique_ptr<StructTypeInfo> typeInfo);

    static KUZU_API std::unique_ptr<LogicalType> REL(std::unique_ptr<StructTypeInfo> typeInfo);

    static KUZU_API std::unique_ptr<LogicalType> RDF_VARIANT();

    static KUZU_API std::unique_ptr<LogicalType> UNION(std::vector<StructField>&& fields);

    static KUZU_API std::unique_ptr<LogicalType> LIST(std::unique_ptr<LogicalType> childType);
    template<class T>
    static inline std::unique_ptr<LogicalType> LIST(T&& childType) {
        return LogicalType::LIST(std::make_unique<LogicalType>(std::forward<T>(childType)));
    }

    static KUZU_API std::unique_ptr<LogicalType> MAP(std::unique_ptr<LogicalType> keyType,
        std::unique_ptr<LogicalType> valueType);
    template<class T>
    static std::unique_ptr<LogicalType> MAP(T&& keyType, T&& valueType) {
        return LogicalType::MAP(std::make_unique<LogicalType>(std::forward<T>(keyType)),
            std::make_unique<LogicalType>(std::forward<T>(valueType)));
    }

    static KUZU_API std::unique_ptr<LogicalType> ARRAY(std::unique_ptr<LogicalType> childType,
        uint64_t numElements);
    template<class T>
    static std::unique_ptr<LogicalType> ARRAY(T&& childType, uint64_t numElements) {
        return LogicalType::ARRAY(std::make_unique<LogicalType>(std::forward<T>(childType)),
            numElements);
    }

private:
    friend struct CAPIHelper;
    friend struct JavaAPIHelper;
    friend class kuzu::processor::ParquetReader;
    explicit LogicalType(LogicalTypeID typeID, std::unique_ptr<ExtraTypeInfo> extraTypeInfo);

private:
    LogicalTypeID typeID;
    PhysicalTypeID physicalType;
    std::unique_ptr<ExtraTypeInfo> extraTypeInfo;
};

class ExtraTypeInfo {
public:
    virtual ~ExtraTypeInfo() = default;

    void serialize(Serializer& serializer) const { serializeInternal(serializer); }

    virtual bool containsAny() const = 0;

    virtual bool operator==(const ExtraTypeInfo& other) const = 0;

    virtual std::unique_ptr<ExtraTypeInfo> copy() const = 0;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const ExtraTypeInfo*, const TARGET*>(this);
    }

protected:
    virtual void serializeInternal(Serializer& serializer) const = 0;
};

class ListTypeInfo : public ExtraTypeInfo {
public:
    ListTypeInfo() = default;
    explicit ListTypeInfo(std::unique_ptr<LogicalType> childType)
        : childType{std::move(childType)} {}

    LogicalType getChildType() const { return *childType; }

    bool containsAny() const override;

    bool operator==(const ExtraTypeInfo& other) const override;

    std::unique_ptr<ExtraTypeInfo> copy() const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);

protected:
    void serializeInternal(Serializer& serializer) const override;

protected:
    std::unique_ptr<LogicalType> childType;
};

class ArrayTypeInfo : public ListTypeInfo {
public:
    ArrayTypeInfo() = default;
    explicit ArrayTypeInfo(std::unique_ptr<LogicalType> childType, uint64_t numElements)
        : ListTypeInfo{std::move(childType)}, numElements{numElements} {}

    uint64_t getNumElements() const { return numElements; }

    bool operator==(const ExtraTypeInfo& other) const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);

    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    void serializeInternal(Serializer& serializer) const override;

private:
    uint64_t numElements;
};

class StructField {
public:
    StructField() : type{std::make_unique<LogicalType>()} {}
    StructField(std::string name, std::unique_ptr<LogicalType> type)
        : name{std::move(name)}, type{std::move(type)} {};

    std::string getName() const { return name; }

    LogicalType getType() const { return *type; }

    bool containsAny() const;

    bool operator==(const StructField& other) const;
    bool operator!=(const StructField& other) const { return !(*this == other); }

    void serialize(Serializer& serializer) const;

    static StructField deserialize(Deserializer& deserializer);

    StructField copy() const;

private:
    std::string name;
    std::unique_ptr<LogicalType> type;
};

class StructTypeInfo : public ExtraTypeInfo {
public:
    StructTypeInfo() = default;
    explicit StructTypeInfo(std::vector<StructField>&& fields);
    StructTypeInfo(const std::vector<std::string>& fieldNames,
        const std::vector<std::unique_ptr<LogicalType>>& fieldTypes);

    bool hasField(const std::string& fieldName) const;
    struct_field_idx_t getStructFieldIdx(std::string fieldName) const;
    StructField getStructField(struct_field_idx_t idx) const;
    StructField getStructField(const std::string& fieldName) const;
    std::vector<StructField> getStructFields() const;

    LogicalType getChildType(struct_field_idx_t idx) const;
    std::vector<LogicalType> getChildrenTypes() const;
    std::vector<std::string> getChildrenNames() const;

    bool containsAny() const override;

    bool operator==(const ExtraTypeInfo& other) const override;

    static std::unique_ptr<ExtraTypeInfo> deserialize(Deserializer& deserializer);
    std::unique_ptr<ExtraTypeInfo> copy() const override;

private:
    void serializeInternal(Serializer& serializer) const override;

private:
    std::vector<StructField> fields;
    std::unordered_map<std::string, struct_field_idx_t> fieldNameToIdxMap;
};

using logical_type_vec_t = std::vector<LogicalType>;

struct ListType {
    static LogicalType getChildType(const LogicalType& type);
};

struct KUZU_API ArrayType {
    static LogicalType getChildType(const LogicalType& type);
    static uint64_t getNumElements(const LogicalType& type);
};

struct StructType {
    static std::vector<LogicalType> getFieldTypes(const LogicalType& type);

    static std::vector<std::string> getFieldNames(const LogicalType& type);

    static uint64_t getNumFields(const LogicalType& type);

    static std::vector<StructField> getFields(const LogicalType& type);

    static bool hasField(const LogicalType& type, const std::string& key);

    static StructField getField(const LogicalType& type, struct_field_idx_t idx);

    static StructField getField(const LogicalType& type, const std::string& key);

    static struct_field_idx_t getFieldIdx(const LogicalType& type, const std::string& key);
};

struct MapType {
    static LogicalType getKeyType(const LogicalType& type);

    static LogicalType getValueType(const LogicalType& type);
};

struct UnionType {
    static constexpr union_field_idx_t TAG_FIELD_IDX = 0;

    static constexpr LogicalTypeID TAG_FIELD_TYPE = LogicalTypeID::INT8;

    static constexpr char TAG_FIELD_NAME[] = "tag";

    static union_field_idx_t getInternalFieldIdx(union_field_idx_t idx);

    static std::string getFieldName(const LogicalType& type, union_field_idx_t idx);

    static LogicalType getFieldType(const LogicalType& type, union_field_idx_t idx);

    static uint64_t getNumFields(const LogicalType& type);
};

struct PhysicalTypeUtils {
    static std::string physicalTypeToString(PhysicalTypeID physicalType);
    static uint32_t getFixedTypeSize(PhysicalTypeID physicalType);
};

struct LogicalTypeUtils {
    KUZU_API static std::string toString(LogicalTypeID dataTypeID);
    KUZU_API static std::string toString(const std::vector<LogicalType>& dataTypes);
    KUZU_API static std::string toString(const std::vector<LogicalTypeID>& dataTypeIDs);
    static LogicalTypeID fromStringToID(const std::string& str);
    static uint32_t getRowLayoutSize(const LogicalType& logicalType);
    static bool isDate(const LogicalType& dataType);
    static bool isDate(const LogicalTypeID& dataType);
    static bool isTimestamp(const LogicalType& dataType);
    static bool isTimestamp(const LogicalTypeID& dataType);
    static bool isUnsigned(const LogicalType& dataType);
    static bool isUnsigned(const LogicalTypeID& dataType);
    static bool isIntegral(const LogicalType& dataType);
    static bool isIntegral(const LogicalTypeID& dataType);
    static bool isNumerical(const LogicalType& dataType);
    static bool isNumerical(const LogicalTypeID& dataType);
    static bool isNested(const LogicalType& dataType);
    static bool isNested(LogicalTypeID logicalTypeID);
    static std::vector<LogicalTypeID> getAllValidComparableLogicalTypes();
    static std::vector<LogicalTypeID> getNumericalLogicalTypeIDs();
    static std::vector<LogicalTypeID> getIntegerTypeIDs();
    static std::vector<LogicalTypeID> getAllValidLogicTypes();
    static bool tryGetMaxLogicalTypeID(const LogicalTypeID& left, const LogicalTypeID& right,
        LogicalTypeID& result);
    static bool tryGetMaxLogicalType(const LogicalType& left, const LogicalType& right,
        LogicalType& result);
    static bool tryGetMaxLogicalType(const std::vector<LogicalType>& types, LogicalType& result);
};

enum class FileVersionType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

} // namespace common
} // namespace kuzu
