#include "common/types/types.h"

#include "common/cast.h"
#include "common/constants.h"
#include "common/exception/binder.h"
#include "common/exception/conversion.h"
#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "common/null_buffer.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"
#include "common/types/int128_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_list.h"
#include "common/types/ku_string.h"
#include "function/built_in_function_utils.h"

using kuzu::function::BuiltInFunctionsUtils;

namespace kuzu {
namespace common {

LogicalType ListType::getChildType(const kuzu::common::LogicalType& type) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::LIST ||
              type.getPhysicalType() == PhysicalTypeID::ARRAY);
    auto listTypeInfo = type.extraTypeInfo->constPtrCast<ListTypeInfo>();
    return listTypeInfo->getChildType();
}

LogicalType ArrayType::getChildType(const LogicalType& type) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::ARRAY);
    auto arrayTypeInfo = type.extraTypeInfo->constPtrCast<ArrayTypeInfo>();
    return arrayTypeInfo->getChildType();
}

uint64_t ArrayType::getNumElements(const LogicalType& type) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::ARRAY);
    auto arrayTypeInfo = type.extraTypeInfo->constPtrCast<ArrayTypeInfo>();
    return arrayTypeInfo->getNumElements();
}

std::vector<LogicalType> StructType::getFieldTypes(const LogicalType& type) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structTypeInfo = type.extraTypeInfo->constPtrCast<StructTypeInfo>();
    return structTypeInfo->getChildrenTypes();
}

std::vector<std::string> StructType::getFieldNames(const LogicalType& type) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structTypeInfo = type.extraTypeInfo->constPtrCast<StructTypeInfo>();
    return structTypeInfo->getChildrenNames();
}

uint64_t StructType::getNumFields(const LogicalType& type) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::STRUCT);
    return getFieldTypes(type).size();
}

std::vector<StructField> StructType::getFields(const LogicalType& type) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structTypeInfo = type.extraTypeInfo->constPtrCast<StructTypeInfo>();
    return structTypeInfo->getStructFields();
}

bool StructType::hasField(const LogicalType& type, const std::string& key) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structTypeInfo = type.extraTypeInfo->constPtrCast<StructTypeInfo>();
    return structTypeInfo->hasField(key);
}

StructField StructType::getField(const LogicalType& type, struct_field_idx_t idx) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structTypeInfo = type.extraTypeInfo->constPtrCast<StructTypeInfo>();
    return structTypeInfo->getStructField(idx);
}

StructField StructType::getField(const LogicalType& type, const std::string& key) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structTypeInfo = type.extraTypeInfo->constPtrCast<StructTypeInfo>();
    return structTypeInfo->getStructField(key);
}

struct_field_idx_t StructType::getFieldIdx(const LogicalType& type, const std::string& key) {
    KU_ASSERT(type.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structTypeInfo = type.extraTypeInfo->constPtrCast<StructTypeInfo>();
    return structTypeInfo->getStructFieldIdx(key);
}

LogicalType MapType::getKeyType(const LogicalType& type) {
    KU_ASSERT(type.getLogicalTypeID() == LogicalTypeID::MAP);
    return StructType::getFieldTypes(ListType::getChildType(type))[0];
}

LogicalType MapType::getValueType(const LogicalType& type) {
    KU_ASSERT(type.getLogicalTypeID() == LogicalTypeID::MAP);
    return StructType::getFieldTypes(ListType::getChildType(type))[1];
}

union_field_idx_t UnionType::getInternalFieldIdx(union_field_idx_t idx) {
    return idx + 1;
}

std::string UnionType::getFieldName(const LogicalType& type, union_field_idx_t idx) {
    KU_ASSERT(type.getLogicalTypeID() == LogicalTypeID::UNION);
    return StructType::getFieldNames(type)[getInternalFieldIdx(idx)];
}

LogicalType UnionType::getFieldType(const LogicalType& type, union_field_idx_t idx) {
    KU_ASSERT(type.getLogicalTypeID() == LogicalTypeID::UNION);
    return StructType::getFieldTypes(type)[getInternalFieldIdx(idx)];
}

uint64_t UnionType::getNumFields(const LogicalType& type) {
    KU_ASSERT(type.getLogicalTypeID() == LogicalTypeID::UNION);
    return StructType::getNumFields(type) - 1;
}

std::string PhysicalTypeUtils::physicalTypeToString(PhysicalTypeID physicalType) {
    // LCOV_EXCL_START
    switch (physicalType) {
    case PhysicalTypeID::BOOL:
        return "BOOL";
    case PhysicalTypeID::INT64:
        return "INT64";
    case PhysicalTypeID::INT32:
        return "INT32";
    case PhysicalTypeID::INT16:
        return "INT16";
    case PhysicalTypeID::INT8:
        return "INT8";
    case PhysicalTypeID::UINT64:
        return "UINT64";
    case PhysicalTypeID::UINT32:
        return "UINT32";
    case PhysicalTypeID::UINT16:
        return "UINT16";
    case PhysicalTypeID::UINT8:
        return "UINT8";
    case PhysicalTypeID::INT128:
        return "INT128";
    case PhysicalTypeID::DOUBLE:
        return "DOUBLE";
    case PhysicalTypeID::FLOAT:
        return "FLOAT";
    case PhysicalTypeID::INTERVAL:
        return "INTERVAL";
    case PhysicalTypeID::INTERNAL_ID:
        return "INTERNAL_ID";
    case PhysicalTypeID::STRING:
        return "STRING";
    case PhysicalTypeID::STRUCT:
        return "STRUCT";
    case PhysicalTypeID::LIST:
        return "LIST";
    case PhysicalTypeID::ARRAY:
        return "ARRAY";
    case PhysicalTypeID::POINTER:
        return "POINTER";
    default:
        KU_UNREACHABLE;
    }
    // LCOV_EXCL_STOP
}

uint32_t PhysicalTypeUtils::getFixedTypeSize(PhysicalTypeID physicalType) {
    switch (physicalType) {
    case PhysicalTypeID::BOOL:
        return sizeof(bool);
    case PhysicalTypeID::INT64:
        return sizeof(int64_t);
    case PhysicalTypeID::INT32:
        return sizeof(int32_t);
    case PhysicalTypeID::INT16:
        return sizeof(int16_t);
    case PhysicalTypeID::INT8:
        return sizeof(int8_t);
    case PhysicalTypeID::UINT64:
        return sizeof(uint64_t);
    case PhysicalTypeID::UINT32:
        return sizeof(uint32_t);
    case PhysicalTypeID::UINT16:
        return sizeof(uint16_t);
    case PhysicalTypeID::UINT8:
        return sizeof(uint8_t);
    case PhysicalTypeID::INT128:
        return sizeof(int128_t);
    case PhysicalTypeID::DOUBLE:
        return sizeof(double);
    case PhysicalTypeID::FLOAT:
        return sizeof(float);
    case PhysicalTypeID::INTERVAL:
        return sizeof(interval_t);
    case PhysicalTypeID::INTERNAL_ID:
        return sizeof(internalID_t);
    default:
        KU_UNREACHABLE;
    }
}

bool ListTypeInfo::containsAny() const {
    return childType->containsAny();
}

bool ListTypeInfo::operator==(const ExtraTypeInfo& other) const {
    auto otherListTypeInfo = ku_dynamic_cast<const ExtraTypeInfo*, const ListTypeInfo*>(&other);
    if (otherListTypeInfo) {
        return *childType == *otherListTypeInfo->childType;
    }
    return false;
}

std::unique_ptr<ExtraTypeInfo> ListTypeInfo::copy() const {
    return std::make_unique<ListTypeInfo>(childType->copy());
}

std::unique_ptr<ExtraTypeInfo> ListTypeInfo::deserialize(Deserializer& deserializer) {
    return std::make_unique<ListTypeInfo>(LogicalType::deserialize(deserializer));
}

void ListTypeInfo::serializeInternal(Serializer& serializer) const {
    childType->serialize(serializer);
}

bool ArrayTypeInfo::operator==(const ExtraTypeInfo& other) const {
    auto otherArrayTypeInfo = ku_dynamic_cast<const ExtraTypeInfo*, const ArrayTypeInfo*>(&other);
    if (otherArrayTypeInfo) {
        return *childType == *otherArrayTypeInfo->childType &&
               numElements == otherArrayTypeInfo->numElements;
    }
    return false;
}

std::unique_ptr<ExtraTypeInfo> ArrayTypeInfo::deserialize(Deserializer& deserializer) {
    auto childType = LogicalType::deserialize(deserializer);
    uint64_t numElements;
    deserializer.deserializeValue(numElements);
    return std::make_unique<ArrayTypeInfo>(std::move(childType), numElements);
}

std::unique_ptr<ExtraTypeInfo> ArrayTypeInfo::copy() const {
    return std::make_unique<ArrayTypeInfo>(childType->copy(), numElements);
}

void ArrayTypeInfo::serializeInternal(Serializer& serializer) const {
    ListTypeInfo::serializeInternal(serializer);
    serializer.serializeValue(numElements);
}

bool StructField::containsAny() const {
    return type->containsAny();
}

bool StructField::operator==(const StructField& other) const {
    return *type == *other.type;
}

void StructField::serialize(Serializer& serializer) const {
    serializer.serializeValue(name);
    type->serialize(serializer);
}

StructField StructField::deserialize(Deserializer& deserializer) {
    std::string name;
    deserializer.deserializeValue(name);
    auto type = LogicalType::deserialize(deserializer);
    return StructField(std::move(name), std::move(type));
}

StructField StructField::copy() const {
    return StructField(name, type->copy());
}

StructTypeInfo::StructTypeInfo(std::vector<StructField>&& fields) : fields{std::move(fields)} {
    for (auto i = 0u; i < this->fields.size(); i++) {
        auto fieldName = this->fields[i].getName();
        StringUtils::toUpper(fieldName);
        fieldNameToIdxMap.emplace(std::move(fieldName), i);
    }
}

StructTypeInfo::StructTypeInfo(const std::vector<std::string>& fieldNames,
    const std::vector<std::unique_ptr<LogicalType>>& fieldTypes) {
    for (auto i = 0u; i < fieldNames.size(); ++i) {
        auto fieldName = fieldNames[i];
        auto normalizedFieldName = fieldName;
        StringUtils::toUpper(normalizedFieldName);
        fieldNameToIdxMap.emplace(normalizedFieldName, i);
        fields.emplace_back(fieldName, fieldTypes[i]->copy());
    }
}

bool StructTypeInfo::hasField(const std::string& fieldName) const {
    return fieldNameToIdxMap.contains(fieldName);
}

struct_field_idx_t StructTypeInfo::getStructFieldIdx(std::string fieldName) const {
    StringUtils::toUpper(fieldName);
    if (fieldNameToIdxMap.contains(fieldName)) {
        return fieldNameToIdxMap.at(fieldName);
    }
    return INVALID_STRUCT_FIELD_IDX;
}

StructField StructTypeInfo::getStructField(struct_field_idx_t idx) const {
    return fields[idx].copy();
}

StructField StructTypeInfo::getStructField(const std::string& fieldName) const {
    auto idx = getStructFieldIdx(fieldName);
    if (idx == INVALID_STRUCT_FIELD_IDX) {
        throw BinderException("Cannot find field " + fieldName + " in STRUCT.");
    }
    return fields[idx].copy();
}

LogicalType StructTypeInfo::getChildType(kuzu::common::struct_field_idx_t idx) const {
    return fields[idx].getType();
}

std::vector<LogicalType> StructTypeInfo::getChildrenTypes() const {
    std::vector<LogicalType> childrenTypesToReturn{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        childrenTypesToReturn[i] = fields[i].getType();
    }
    return childrenTypesToReturn;
}

std::vector<std::string> StructTypeInfo::getChildrenNames() const {
    std::vector<std::string> childrenNames{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        childrenNames[i] = fields[i].getName();
    }
    return childrenNames;
}

std::vector<StructField> StructTypeInfo::getStructFields() const {
    std::vector<StructField> structFields{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        structFields[i] = fields[i].copy();
    }
    return structFields;
}

bool StructTypeInfo::containsAny() const {
    for (auto& field : fields) {
        if (field.containsAny()) {
            return true;
        }
    }
    return false;
}

bool StructTypeInfo::operator==(const ExtraTypeInfo& other) const {
    auto otherStructTypeInfo = ku_dynamic_cast<const ExtraTypeInfo*, const StructTypeInfo*>(&other);
    if (otherStructTypeInfo) {
        if (fields.size() != otherStructTypeInfo->fields.size()) {
            return false;
        }
        for (auto i = 0u; i < fields.size(); ++i) {
            if (fields[i] != otherStructTypeInfo->fields[i]) {
                return false;
            }
        }
        return true;
    }
    return false;
}

std::unique_ptr<ExtraTypeInfo> StructTypeInfo::deserialize(Deserializer& deserializer) {
    std::vector<StructField> fields;
    deserializer.deserializeVector(fields);
    return std::make_unique<StructTypeInfo>(std::move(fields));
}

std::unique_ptr<ExtraTypeInfo> StructTypeInfo::copy() const {
    std::vector<StructField> structFields{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        structFields[i] = fields[i].copy();
    }
    return std::make_unique<StructTypeInfo>(std::move(structFields));
}

void StructTypeInfo::serializeInternal(Serializer& serializer) const {
    serializer.serializeVector(fields);
}

static std::string getIncompleteTypeErrMsg(LogicalTypeID id) {
    return "Trying to create nested type " + LogicalTypeUtils::toString(id) +
           " without child information.";
}

LogicalType::LogicalType(LogicalTypeID typeID) : typeID{typeID}, extraTypeInfo{nullptr} {
    physicalType = getPhysicalType(typeID);
    // LCOV_EXCL_START
    switch (physicalType) {
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::ARRAY:
        throw BinderException(getIncompleteTypeErrMsg(typeID));
    case PhysicalTypeID::STRUCT: {
        switch (typeID) {
        // Node/Rel types are exempted due to some complex code in bind_graph_pattern.cpp
        case LogicalTypeID::NODE:
        case LogicalTypeID::REL:
        case LogicalTypeID::RECURSIVE_REL:
            return;
        default:
            throw BinderException(getIncompleteTypeErrMsg(typeID));
        }
    }
    default:
        return;
    }
    // LCOV_EXCL_STOP
}

LogicalType::LogicalType(LogicalTypeID typeID, std::unique_ptr<ExtraTypeInfo> extraTypeInfo)
    : typeID{typeID}, extraTypeInfo{std::move(extraTypeInfo)} {
    physicalType = getPhysicalType(typeID);
}

LogicalType::LogicalType(const LogicalType& other) {
    typeID = other.typeID;
    physicalType = other.physicalType;
    if (other.extraTypeInfo != nullptr) {
        extraTypeInfo = other.extraTypeInfo->copy();
    }
}

bool LogicalType::containsAny() const {
    if (extraTypeInfo != nullptr) {
        return extraTypeInfo->containsAny();
    }
    return typeID == LogicalTypeID::ANY;
}

LogicalType& LogicalType::operator=(const LogicalType& other) {
    // Reuse the copy constructor and move assignment operator.
    LogicalType copy(other);
    return *this = std::move(copy);
}

bool LogicalType::operator==(const LogicalType& other) const {
    if (typeID != other.typeID) {
        return false;
    }
    if (extraTypeInfo) {
        return *extraTypeInfo == *other.extraTypeInfo;
    }
    return true;
}

bool LogicalType::operator!=(const LogicalType& other) const {
    return !((*this) == other);
}

std::string LogicalType::toString() const {
    switch (typeID) {
    case LogicalTypeID::MAP: {
        auto structType =
            ku_dynamic_cast<ExtraTypeInfo*, ListTypeInfo*>(extraTypeInfo.get())->getChildType();
        auto fieldTypes = StructType::getFieldTypes(structType);
        return "MAP(" + fieldTypes[0].toString() + ", " + fieldTypes[1].toString() + ")";
    }
    case LogicalTypeID::LIST: {
        auto listTypeInfo = ku_dynamic_cast<ExtraTypeInfo*, ListTypeInfo*>(extraTypeInfo.get());
        return listTypeInfo->getChildType().toString() + "[]";
    }
    case LogicalTypeID::ARRAY: {
        auto arrayTypeInfo = ku_dynamic_cast<ExtraTypeInfo*, ArrayTypeInfo*>(extraTypeInfo.get());
        return arrayTypeInfo->getChildType().toString() + "[" +
               std::to_string(arrayTypeInfo->getNumElements()) + "]";
    }
    case LogicalTypeID::UNION: {
        auto unionTypeInfo = ku_dynamic_cast<ExtraTypeInfo*, StructTypeInfo*>(extraTypeInfo.get());
        std::string dataTypeStr = LogicalTypeUtils::toString(typeID) + "(";
        auto numFields = unionTypeInfo->getChildrenTypes().size();
        auto fieldNames = unionTypeInfo->getChildrenNames();
        for (auto i = 1u; i < numFields; i++) {
            dataTypeStr += fieldNames[i] + " ";
            dataTypeStr += unionTypeInfo->getChildType(i).toString();
            dataTypeStr += (i == numFields - 1 ? ")" : ", ");
        }
        return dataTypeStr;
    }
    case LogicalTypeID::STRUCT: {
        auto structTypeInfo = ku_dynamic_cast<ExtraTypeInfo*, StructTypeInfo*>(extraTypeInfo.get());
        std::string dataTypeStr = LogicalTypeUtils::toString(typeID) + "(";
        auto numFields = structTypeInfo->getChildrenTypes().size();
        auto fieldNames = structTypeInfo->getChildrenNames();
        for (auto i = 0u; i < numFields - 1; i++) {
            dataTypeStr += fieldNames[i] + " ";
            dataTypeStr += structTypeInfo->getChildType(i).toString();
            dataTypeStr += ", ";
        }
        dataTypeStr += fieldNames[numFields - 1] + " ";
        dataTypeStr += structTypeInfo->getChildType(numFields - 1).toString();
        return dataTypeStr + ")";
    }
    case LogicalTypeID::ANY:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::BOOL:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
    case LogicalTypeID::INT128:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP_NS:
    case LogicalTypeID::TIMESTAMP_MS:
    case LogicalTypeID::TIMESTAMP_SEC:
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::BLOB:
    case LogicalTypeID::UUID:
    case LogicalTypeID::STRING:
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::RDF_VARIANT:
        return LogicalTypeUtils::toString(typeID);
    default:
        KU_UNREACHABLE;
    }
}

static LogicalTypeID strToLogicalTypeID(const std::string& trimmedStr);
static std::vector<std::string> parseStructFields(const std::string& structTypeStr);
static std::unique_ptr<LogicalType> parseListType(const std::string& trimmedStr);
static std::unique_ptr<LogicalType> parseArrayType(const std::string& trimmedStr);
static std::vector<StructField> parseStructTypeInfo(const std::string& structTypeStr);
static std::unique_ptr<LogicalType> parseStructType(const std::string& trimmedStr);
static std::unique_ptr<LogicalType> parseMapType(const std::string& trimmedStr);
static std::unique_ptr<LogicalType> parseUnionType(const std::string& trimmedStr);

LogicalType LogicalType::fromString(const std::string& str) {
    LogicalType dataType;
    auto trimmedStr = StringUtils::ltrim(StringUtils::rtrim(str));
    auto upperDataTypeString = StringUtils::getUpper(trimmedStr);
    if (upperDataTypeString.ends_with("[]")) {
        dataType = *parseListType(trimmedStr);
    } else if (upperDataTypeString.ends_with("]")) {
        dataType = *parseArrayType(trimmedStr);
    } else if (upperDataTypeString.starts_with("STRUCT")) {
        dataType = *parseStructType(trimmedStr);
    } else if (upperDataTypeString.starts_with("MAP")) {
        dataType = *parseMapType(trimmedStr);
    } else if (upperDataTypeString.starts_with("UNION")) {
        dataType = *parseUnionType(trimmedStr);
    } else if (upperDataTypeString == "RDF_VARIANT") {
        dataType = *LogicalType::RDF_VARIANT();
    } else {
        dataType.typeID = strToLogicalTypeID(upperDataTypeString);
    }
    dataType.physicalType = LogicalType::getPhysicalType(dataType.typeID);
    return dataType;
}

void LogicalType::serialize(Serializer& serializer) const {
    serializer.serializeValue(typeID);
    serializer.serializeValue(physicalType);
    switch (physicalType) {
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::STRUCT:
        extraTypeInfo->serialize(serializer);
    default:
        break;
    }
}

std::unique_ptr<LogicalType> LogicalType::deserialize(Deserializer& deserializer) {
    LogicalTypeID typeID;
    deserializer.deserializeValue(typeID);
    PhysicalTypeID physicalType;
    deserializer.deserializeValue(physicalType);
    std::unique_ptr<ExtraTypeInfo> extraTypeInfo;
    switch (physicalType) {
    case PhysicalTypeID::LIST: {
        extraTypeInfo = ListTypeInfo::deserialize(deserializer);
    } break;
    case PhysicalTypeID::ARRAY: {
        extraTypeInfo = ArrayTypeInfo::deserialize(deserializer);
    } break;
    case PhysicalTypeID::STRUCT: {
        extraTypeInfo = StructTypeInfo::deserialize(deserializer);
    } break;
    default:
        extraTypeInfo = nullptr;
    }
    auto result = std::make_unique<LogicalType>();
    result->typeID = typeID;
    result->physicalType = physicalType;
    result->extraTypeInfo = std::move(extraTypeInfo);
    return result;
}

std::unique_ptr<LogicalType> LogicalType::copy() const {
    if (extraTypeInfo != nullptr) {
        return std::unique_ptr<LogicalType>(new LogicalType(typeID, extraTypeInfo->copy()));
    }
    return std::make_unique<LogicalType>(typeID);
}

std::vector<std::unique_ptr<LogicalType>> LogicalType::copy(
    const std::vector<std::unique_ptr<LogicalType>>& types) {
    std::vector<std::unique_ptr<LogicalType>> typesCopy;
    for (auto& type : types) {
        typesCopy.push_back(type->copy());
    }
    return typesCopy;
}

std::vector<LogicalType> LogicalType::copy(const std::vector<LogicalType>& types) {
    std::vector<LogicalType> typesCopy;
    typesCopy.reserve(types.size());
    for (auto& type : types) {
        typesCopy.push_back(*type.copy());
    }
    return typesCopy;
}

std::vector<LogicalType> LogicalType::copy(const std::vector<LogicalType*>& types) {
    std::vector<LogicalType> typesCopy;
    typesCopy.reserve(types.size());
    for (auto& type : types) {
        typesCopy.push_back(*type->copy());
    }
    return typesCopy;
}

PhysicalTypeID LogicalType::getPhysicalType(LogicalTypeID typeID) {
    switch (typeID) {
    case LogicalTypeID::ANY: {
        return PhysicalTypeID::ANY;
    } break;
    case LogicalTypeID::BOOL: {
        return PhysicalTypeID::BOOL;
    } break;
    case LogicalTypeID::TIMESTAMP_MS:
    case LogicalTypeID::TIMESTAMP_NS:
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::TIMESTAMP_SEC:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        return PhysicalTypeID::INT64;
    } break;
    case LogicalTypeID::DATE:
    case LogicalTypeID::INT32: {
        return PhysicalTypeID::INT32;
    } break;
    case LogicalTypeID::INT16: {
        return PhysicalTypeID::INT16;
    } break;
    case LogicalTypeID::INT8: {
        return PhysicalTypeID::INT8;
    } break;
    case LogicalTypeID::UINT64: {
        return PhysicalTypeID::UINT64;
    } break;
    case LogicalTypeID::UINT32: {
        return PhysicalTypeID::UINT32;
    } break;
    case LogicalTypeID::UINT16: {
        return PhysicalTypeID::UINT16;
    } break;
    case LogicalTypeID::UINT8: {
        return PhysicalTypeID::UINT8;
    } break;
    case LogicalTypeID::UUID:
    case LogicalTypeID::INT128: {
        return PhysicalTypeID::INT128;
    } break;
    case LogicalTypeID::DOUBLE: {
        return PhysicalTypeID::DOUBLE;
    } break;
    case LogicalTypeID::FLOAT: {
        return PhysicalTypeID::FLOAT;
    } break;
    case LogicalTypeID::INTERVAL: {
        return PhysicalTypeID::INTERVAL;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        return PhysicalTypeID::INTERNAL_ID;
    } break;
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        return PhysicalTypeID::STRING;
    } break;
    case LogicalTypeID::MAP:
    case LogicalTypeID::LIST: {
        return PhysicalTypeID::LIST;
    } break;
    case LogicalTypeID::ARRAY: {
        return PhysicalTypeID::ARRAY;
    } break;
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::UNION:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::RDF_VARIANT: {
        return PhysicalTypeID::STRUCT;
    } break;
    case LogicalTypeID::POINTER: {
        return PhysicalTypeID::POINTER;
    } break;
    default:
        KU_UNREACHABLE;
    }
}

LogicalTypeID strToLogicalTypeID(const std::string& str) {
    auto upperStr = StringUtils::getUpper(str);
    if ("INTERNAL_ID" == upperStr) {
        return LogicalTypeID::INTERNAL_ID;
    } else if ("INT64" == upperStr) {
        return LogicalTypeID::INT64;
    } else if ("INT32" == upperStr || "INT" == upperStr) {
        return LogicalTypeID::INT32;
    } else if ("INT16" == upperStr) {
        return LogicalTypeID::INT16;
    } else if ("INT8" == upperStr) {
        return LogicalTypeID::INT8;
    } else if ("UINT64" == upperStr) {
        return LogicalTypeID::UINT64;
    } else if ("UINT32" == upperStr) {
        return LogicalTypeID::UINT32;
    } else if ("UINT16" == upperStr) {
        return LogicalTypeID::UINT16;
    } else if ("UINT8" == upperStr) {
        return LogicalTypeID::UINT8;
    } else if ("INT128" == upperStr) {
        return LogicalTypeID::INT128;
    } else if ("DOUBLE" == upperStr || "FLOAT8" == upperStr) {
        return LogicalTypeID::DOUBLE;
    } else if ("FLOAT" == upperStr || "FLOAT4" == upperStr || "REAL" == upperStr) {
        return LogicalTypeID::FLOAT;
    } else if ("BOOLEAN" == upperStr || "BOOL" == upperStr) {
        return LogicalTypeID::BOOL;
    } else if ("BYTEA" == upperStr || "BLOB" == upperStr) {
        return LogicalTypeID::BLOB;
    } else if ("UUID" == upperStr) {
        return LogicalTypeID::UUID;
    } else if ("STRING" == upperStr) {
        return LogicalTypeID::STRING;
    } else if ("DATE" == upperStr) {
        return LogicalTypeID::DATE;
    } else if ("TIMESTAMP" == upperStr) {
        return LogicalTypeID::TIMESTAMP;
    } else if ("TIMESTAMP_NS" == upperStr) {
        return LogicalTypeID::TIMESTAMP_NS;
    } else if ("TIMESTAMP_MS" == upperStr) {
        return LogicalTypeID::TIMESTAMP_MS;
    } else if ("TIMESTAMP_SEC" == upperStr || "TIMESTAMP_S" == upperStr) {
        return LogicalTypeID::TIMESTAMP_SEC;
    } else if ("TIMESTAMP_TZ" == upperStr) {
        return LogicalTypeID::TIMESTAMP_TZ;
    } else if ("INTERVAL" == upperStr || "DURATION" == upperStr) {
        return LogicalTypeID::INTERVAL;
    } else if ("SERIAL" == upperStr) {
        return LogicalTypeID::SERIAL;
    } else {
        throw NotImplementedException("Cannot parse dataTypeID: " + str);
    }
}

std::string LogicalTypeUtils::toString(LogicalTypeID dataTypeID) {
    // LCOV_EXCL_START
    switch (dataTypeID) {
    case LogicalTypeID::ANY:
        return "ANY";
    case LogicalTypeID::NODE:
        return "NODE";
    case LogicalTypeID::REL:
        return "REL";
    case LogicalTypeID::RECURSIVE_REL:
        return "RECURSIVE_REL";
    case LogicalTypeID::INTERNAL_ID:
        return "INTERNAL_ID";
    case LogicalTypeID::BOOL:
        return "BOOL";
    case LogicalTypeID::INT64:
        return "INT64";
    case LogicalTypeID::INT32:
        return "INT32";
    case LogicalTypeID::INT16:
        return "INT16";
    case LogicalTypeID::INT8:
        return "INT8";
    case LogicalTypeID::UINT64:
        return "UINT64";
    case LogicalTypeID::UINT32:
        return "UINT32";
    case LogicalTypeID::UINT16:
        return "UINT16";
    case LogicalTypeID::UINT8:
        return "UINT8";
    case LogicalTypeID::INT128:
        return "INT128";
    case LogicalTypeID::DOUBLE:
        return "DOUBLE";
    case LogicalTypeID::FLOAT:
        return "FLOAT";
    case LogicalTypeID::DATE:
        return "DATE";
    case LogicalTypeID::TIMESTAMP_NS:
        return "TIMESTAMP_NS";
    case LogicalTypeID::TIMESTAMP_MS:
        return "TIMESTAMP_MS";
    case LogicalTypeID::TIMESTAMP_SEC:
        return "TIMESTAMP_SEC";
    case LogicalTypeID::TIMESTAMP_TZ:
        return "TIMESTAMP_TZ";
    case LogicalTypeID::TIMESTAMP:
        return "TIMESTAMP";
    case LogicalTypeID::INTERVAL:
        return "INTERVAL";
    case LogicalTypeID::BLOB:
        return "BLOB";
    case LogicalTypeID::UUID:
        return "UUID";
    case LogicalTypeID::STRING:
        return "STRING";
    case LogicalTypeID::LIST:
        return "LIST";
    case LogicalTypeID::ARRAY:
        return "ARRAY";
    case LogicalTypeID::STRUCT:
        return "STRUCT";
    case LogicalTypeID::RDF_VARIANT:
        return "RDF_VARIANT";
    case LogicalTypeID::SERIAL:
        return "SERIAL";
    case LogicalTypeID::MAP:
        return "MAP";
    case LogicalTypeID::UNION:
        return "UNION";
    case LogicalTypeID::POINTER:
        return "POINTER";
    default:
        KU_UNREACHABLE;
    }
    // LCOV_EXCL_STOP
}

LogicalTypeID LogicalTypeUtils::fromStringToID(const std::string& str) {
    if (str == "ANY") {
        return LogicalTypeID::ANY;
    } else if (str == "NODE") {
        return LogicalTypeID::NODE;
    } else if (str == "REL") {
        return LogicalTypeID::REL;
    } else if (str == "RECURSIVE_REL") {
        return LogicalTypeID::RECURSIVE_REL;
    } else if (str == "INTERNAL_ID") {
        return LogicalTypeID::INTERNAL_ID;
    } else if (str == "BOOL") {
        return LogicalTypeID::BOOL;
    } else if (str == "INT64") {
        return LogicalTypeID::INT64;
    } else if (str == "INT32") {
        return LogicalTypeID::INT32;
    } else if (str == "INT16") {
        return LogicalTypeID::INT16;
    } else if (str == "INT8") {
        return LogicalTypeID::INT8;
    } else if (str == "UINT64") {
        return LogicalTypeID::UINT64;
    } else if (str == "UINT32") {
        return LogicalTypeID::UINT32;
    } else if (str == "UINT16") {
        return LogicalTypeID::UINT16;
    } else if (str == "UINT8") {
        return LogicalTypeID::UINT8;
    } else if (str == "INT128") {
        return LogicalTypeID::INT128;
    } else if (str == "DOUBLE") {
        return LogicalTypeID::DOUBLE;
    } else if (str == "FLOAT") {
        return LogicalTypeID::FLOAT;
    } else if (str == "DATE") {
        return LogicalTypeID::DATE;
    } else if (str == "TIMESTAMP_NS") {
        return LogicalTypeID::TIMESTAMP_NS;
    } else if (str == "TIMESTAMP_MS") {
        return LogicalTypeID::TIMESTAMP_MS;
    } else if (str == "TIMESTAMP_SEC") {
        return LogicalTypeID::TIMESTAMP_SEC;
    } else if (str == "TIMESTAMP_TZ") {
        return LogicalTypeID::TIMESTAMP_TZ;
    } else if (str == "TIMESTAMP") {
        return LogicalTypeID::TIMESTAMP;
    } else if (str == "INTERVAL") {
        return LogicalTypeID::INTERVAL;
    } else if (str == "BLOB") {
        return LogicalTypeID::BLOB;
    } else if (str == "UUID") {
        return LogicalTypeID::UUID;
    } else if (str == "STRING") {
        return LogicalTypeID::STRING;
    } else if (str == "LIST") {
        return LogicalTypeID::LIST;
    } else if (str == "ARRAY") {
        return LogicalTypeID::ARRAY;
    } else if (str == "STRUCT") {
        return LogicalTypeID::STRUCT;
    } else if (str == "RDF_VARIANT") {
        return LogicalTypeID::RDF_VARIANT;
    } else if (str == "SERIAL") {
        return LogicalTypeID::SERIAL;
    } else if (str == "MAP") {
        return LogicalTypeID::MAP;
    } else if (str == "UNION") {
        return LogicalTypeID::UNION;
    } else if (str == "POINTER") {
        return LogicalTypeID::POINTER;
    } else {
        throw RuntimeException(stringFormat("Unknown type {}", str));
    }
}

std::string LogicalTypeUtils::toString(const std::vector<LogicalType>& dataTypes) {
    if (dataTypes.empty()) {
        return {""};
    }
    std::string result = "(" + dataTypes[0].toString();
    for (auto i = 1u; i < dataTypes.size(); ++i) {
        result += "," + dataTypes[i].toString();
    }
    result += ")";
    return result;
}

std::string LogicalTypeUtils::toString(const std::vector<LogicalTypeID>& dataTypeIDs) {
    if (dataTypeIDs.empty()) {
        return {""};
    }
    std::string result = "(" + LogicalTypeUtils::toString(dataTypeIDs[0]);
    for (auto i = 1u; i < dataTypeIDs.size(); ++i) {
        result += "," + LogicalTypeUtils::toString(dataTypeIDs[i]);
    }
    result += ")";
    return result;
}

uint32_t LogicalTypeUtils::getRowLayoutSize(const LogicalType& type) {
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return sizeof(ku_list_t);
    }
    case PhysicalTypeID::STRUCT: {
        uint32_t size = 0;
        auto fieldsTypes = StructType::getFieldTypes(type);
        for (auto fieldType : fieldsTypes) {
            size += getRowLayoutSize(fieldType);
        }
        size += NullBuffer::getNumBytesForNullValues(fieldsTypes.size());
        return size;
    }
    default:
        return PhysicalTypeUtils::getFixedTypeSize(type.getPhysicalType());
    }
}

bool LogicalTypeUtils::isDate(const LogicalType& dataType) {
    return isDate(dataType.typeID);
}

bool LogicalTypeUtils::isDate(const LogicalTypeID& dataType) {
    return dataType == LogicalTypeID::DATE;
}

bool LogicalTypeUtils::isTimestamp(const LogicalType& dataType) {
    return isTimestamp(dataType.typeID);
}

bool LogicalTypeUtils::isTimestamp(const LogicalTypeID& dataType) {
    switch (dataType) {
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::TIMESTAMP_SEC:
    case LogicalTypeID::TIMESTAMP_MS:
    case LogicalTypeID::TIMESTAMP_NS:
        return true;
    default:
        return false;
    }
}

bool LogicalTypeUtils::isUnsigned(const LogicalType& dataType) {
    return isUnsigned(dataType.typeID);
}

bool LogicalTypeUtils::isUnsigned(const LogicalTypeID& dataType) {
    switch (dataType) {
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
        return true;
    default:
        return false;
    }
}

bool LogicalTypeUtils::isIntegral(const LogicalType& dataType) {
    return isIntegral(dataType.typeID);
}

bool LogicalTypeUtils::isIntegral(const LogicalTypeID& dataType) {
    switch (dataType) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
    case LogicalTypeID::INT128:
    case LogicalTypeID::SERIAL:
        return true;
    default:
        return false;
    }
}

bool LogicalTypeUtils::isNumerical(const LogicalType& dataType) {
    return isNumerical(dataType.typeID);
}

bool LogicalTypeUtils::isNumerical(const LogicalTypeID& dataType) {
    switch (dataType) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
    case LogicalTypeID::INT128:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::SERIAL:
        return true;
    default:
        return false;
    }
}

bool LogicalTypeUtils::isNested(const LogicalType& dataType) {
    return isNested(dataType.typeID);
}

bool LogicalTypeUtils::isNested(kuzu::common::LogicalTypeID logicalTypeID) {
    switch (logicalTypeID) {
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::LIST:
    case LogicalTypeID::ARRAY:
    case LogicalTypeID::UNION:
    case LogicalTypeID::MAP:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::RDF_VARIANT:
        return true;
    default:
        return false;
    }
}

std::vector<LogicalTypeID> LogicalTypeUtils::getAllValidComparableLogicalTypes() {
    return std::vector<LogicalTypeID>{LogicalTypeID::BOOL, LogicalTypeID::INT64,
        LogicalTypeID::INT32, LogicalTypeID::INT16, LogicalTypeID::INT8, LogicalTypeID::UINT64,
        LogicalTypeID::UINT32, LogicalTypeID::UINT16, LogicalTypeID::UINT8, LogicalTypeID::INT128,
        LogicalTypeID::DOUBLE, LogicalTypeID::FLOAT, LogicalTypeID::DATE, LogicalTypeID::TIMESTAMP,
        LogicalTypeID::TIMESTAMP_NS, LogicalTypeID::TIMESTAMP_MS, LogicalTypeID::TIMESTAMP_SEC,
        LogicalTypeID::TIMESTAMP_TZ, LogicalTypeID::INTERVAL, LogicalTypeID::BLOB,
        LogicalTypeID::UUID, LogicalTypeID::STRING, LogicalTypeID::SERIAL};
}

std::vector<LogicalTypeID> LogicalTypeUtils::getIntegerTypeIDs() {
    return std::vector<LogicalTypeID>{LogicalTypeID::INT128, LogicalTypeID::INT64,
        LogicalTypeID::INT32, LogicalTypeID::INT16, LogicalTypeID::INT8, LogicalTypeID::SERIAL,
        LogicalTypeID::UINT64, LogicalTypeID::UINT32, LogicalTypeID::UINT16, LogicalTypeID::UINT8};
}

static std::vector<LogicalTypeID> getFloatingPointTypeIDs() {
    return std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE, LogicalTypeID::FLOAT};
}

std::vector<LogicalTypeID> LogicalTypeUtils::getNumericalLogicalTypeIDs() {
    auto integerTypes = getIntegerTypeIDs();
    auto floatingPointTypes = getFloatingPointTypeIDs();
    integerTypes.insert(integerTypes.end(), floatingPointTypes.begin(), floatingPointTypes.end());
    return integerTypes;
}

std::vector<LogicalTypeID> LogicalTypeUtils::getAllValidLogicTypes() {
    return std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID, LogicalTypeID::BOOL,
        LogicalTypeID::INT64, LogicalTypeID::INT32, LogicalTypeID::INT16, LogicalTypeID::INT8,
        LogicalTypeID::UINT64, LogicalTypeID::UINT32, LogicalTypeID::UINT16, LogicalTypeID::UINT8,
        LogicalTypeID::INT128, LogicalTypeID::DOUBLE, LogicalTypeID::STRING, LogicalTypeID::BLOB,
        LogicalTypeID::UUID, LogicalTypeID::DATE, LogicalTypeID::TIMESTAMP,
        LogicalTypeID::TIMESTAMP_NS, LogicalTypeID::TIMESTAMP_MS, LogicalTypeID::TIMESTAMP_SEC,
        LogicalTypeID::TIMESTAMP_TZ, LogicalTypeID::INTERVAL, LogicalTypeID::LIST,
        LogicalTypeID::ARRAY, LogicalTypeID::MAP, LogicalTypeID::FLOAT, LogicalTypeID::SERIAL,
        LogicalTypeID::NODE, LogicalTypeID::REL, LogicalTypeID::STRUCT, LogicalTypeID::UNION,
        LogicalTypeID::RDF_VARIANT};
}

std::vector<std::string> parseStructFields(const std::string& structTypeStr) {
    std::vector<std::string> structFieldsStr;
    auto startPos = 0u;
    auto curPos = 0u;
    auto numOpenBrackets = 0u;
    while (curPos < structTypeStr.length()) {
        switch (structTypeStr[curPos]) {
        case '(': {
            numOpenBrackets++;
        } break;
        case ')': {
            numOpenBrackets--;
        } break;
        case ',': {
            if (numOpenBrackets == 0) {
                structFieldsStr.push_back(
                    StringUtils::ltrim(structTypeStr.substr(startPos, curPos - startPos)));
                startPos = curPos + 1;
            }
        } break;
        default: {
            // Normal character, continue.
        }
        }
        curPos++;
    }
    structFieldsStr.push_back(
        StringUtils::ltrim(structTypeStr.substr(startPos, curPos - startPos)));
    return structFieldsStr;
}

std::unique_ptr<LogicalType> parseListType(const std::string& trimmedStr) {
    return LogicalType::LIST(LogicalType::fromString(trimmedStr.substr(0, trimmedStr.size() - 2)));
}

std::unique_ptr<LogicalType> parseArrayType(const std::string& trimmedStr) {
    auto leftBracketPos = trimmedStr.find_last_of('[');
    auto rightBracketPos = trimmedStr.find_last_of(']');
    auto childType = std::make_unique<LogicalType>(
        LogicalType::fromString(trimmedStr.substr(0, leftBracketPos)));
    auto numElements = std::strtoll(
        trimmedStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1).c_str(),
        nullptr, 0 /* base */);
    if (numElements <= 0) {
        // Note: the parser already guarantees that the number of elements is a non-negative
        // number. However, we still need to check whether the number of elements is 0.
        throw BinderException("The number of elements in an array must be greater than 0. Given: " +
                              std::to_string(numElements) + ".");
    }
    return LogicalType::ARRAY(std::move(childType), numElements);
}

std::vector<StructField> parseStructTypeInfo(const std::string& structTypeStr) {
    auto leftBracketPos = structTypeStr.find('(');
    auto rightBracketPos = structTypeStr.find_last_of(')');
    if (leftBracketPos == std::string::npos || rightBracketPos == std::string::npos) {
        throw Exception("Cannot parse struct type: " + structTypeStr);
    }
    // Remove the leading and trailing brackets.
    auto structFieldsStr =
        structTypeStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
    std::vector<StructField> structFields;
    auto structFieldStrs = parseStructFields(structFieldsStr);
    for (auto& structFieldStr : structFieldStrs) {
        auto pos = structFieldStr.find(' ');
        auto fieldName = structFieldStr.substr(0, pos);
        auto fieldTypeString = structFieldStr.substr(pos + 1);
        structFields.emplace_back(fieldName,
            std::make_unique<LogicalType>(LogicalType::fromString(fieldTypeString)));
    }
    return structFields;
}

std::unique_ptr<LogicalType> parseStructType(const std::string& trimmedStr) {
    return LogicalType::STRUCT(parseStructTypeInfo(trimmedStr));
}

std::unique_ptr<LogicalType> parseMapType(const std::string& trimmedStr) {
    auto leftBracketPos = trimmedStr.find('(');
    auto rightBracketPos = trimmedStr.find_last_of(')');
    if (leftBracketPos == std::string::npos || rightBracketPos == std::string::npos) {
        throw Exception("Cannot parse map type: " + trimmedStr);
    }
    auto mapTypeStr = trimmedStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
    auto keyValueTypes = StringUtils::splitComma(mapTypeStr);
    return LogicalType::MAP(LogicalType::fromString(keyValueTypes[0]),
        LogicalType::fromString(keyValueTypes[1]));
}

std::unique_ptr<LogicalType> parseUnionType(const std::string& trimmedStr) {
    auto unionFields = parseStructTypeInfo(trimmedStr);
    auto unionTagField = StructField(UnionType::TAG_FIELD_NAME,
        std::make_unique<LogicalType>(UnionType::TAG_FIELD_TYPE));
    unionFields.insert(unionFields.begin(), std::move(unionTagField));
    return LogicalType::UNION(std::move(unionFields));
}

std::unique_ptr<LogicalType> LogicalType::STRUCT(std::vector<StructField>&& fields) {
    return std::unique_ptr<LogicalType>(new LogicalType(LogicalTypeID::STRUCT,
        std::make_unique<StructTypeInfo>(std::move(fields))));
}

std::unique_ptr<LogicalType> LogicalType::RECURSIVE_REL(std::unique_ptr<StructTypeInfo> typeInfo) {
    return std::unique_ptr<LogicalType>(
        new LogicalType(LogicalTypeID::RECURSIVE_REL, std::move(typeInfo)));
}

std::unique_ptr<LogicalType> LogicalType::NODE(std::unique_ptr<StructTypeInfo> typeInfo) {
    return std::unique_ptr<LogicalType>(new LogicalType(LogicalTypeID::NODE, std::move(typeInfo)));
}

std::unique_ptr<LogicalType> LogicalType::REL(std::unique_ptr<StructTypeInfo> typeInfo) {
    return std::unique_ptr<LogicalType>(new LogicalType(LogicalTypeID::REL, std::move(typeInfo)));
}

std::unique_ptr<LogicalType> LogicalType::RDF_VARIANT() {
    std::vector<StructField> fields;
    fields.emplace_back("_type", LogicalType::UINT8());
    fields.emplace_back("_value", LogicalType::BLOB());
    auto extraInfo = std::make_unique<StructTypeInfo>(std::move(fields));
    return std::unique_ptr<LogicalType>(
        new LogicalType(LogicalTypeID::RDF_VARIANT, std::move(extraInfo)));
}

std::unique_ptr<LogicalType> LogicalType::UNION(std::vector<StructField>&& fields) {
    return std::unique_ptr<LogicalType>(
        new LogicalType(LogicalTypeID::UNION, std::make_unique<StructTypeInfo>(std::move(fields))));
}

std::unique_ptr<LogicalType> LogicalType::LIST(std::unique_ptr<LogicalType> childType) {
    return std::unique_ptr<LogicalType>(
        new LogicalType(LogicalTypeID::LIST, std::make_unique<ListTypeInfo>(std::move(childType))));
}

std::unique_ptr<LogicalType> LogicalType::MAP(std::unique_ptr<LogicalType> keyType,
    std::unique_ptr<LogicalType> valueType) {
    std::vector<StructField> structFields;
    structFields.emplace_back(InternalKeyword::MAP_KEY, std::move(keyType));
    structFields.emplace_back(InternalKeyword::MAP_VALUE, std::move(valueType));
    auto mapStructType = LogicalType::STRUCT(std::move(structFields));
    return std::unique_ptr<LogicalType>(new LogicalType(LogicalTypeID::MAP,
        std::make_unique<ListTypeInfo>(std::move(mapStructType))));
}

std::unique_ptr<LogicalType> LogicalType::ARRAY(std::unique_ptr<LogicalType> childType,
    uint64_t numElements) {
    return std::unique_ptr<LogicalType>(new LogicalType(LogicalTypeID::ARRAY,
        std::make_unique<ArrayTypeInfo>(std::move(childType), numElements)));
}

// If we can combine the child types, then we can combine the list
static bool tryCombineListTypes(const LogicalType& left, const LogicalType& right,
    LogicalType& result) {
    LogicalType childType;
    if (!LogicalTypeUtils::tryGetMaxLogicalType(ListType::getChildType(left),
            ListType::getChildType(right), childType)) {
        return false;
    }
    result = *LogicalType::LIST(childType);
    return true;
}

static bool tryCombineArrayTypes(const LogicalType& left, const LogicalType& right,
    LogicalType& result) {
    if (ArrayType::getNumElements(left) != ArrayType::getNumElements(right)) {
        return tryCombineListTypes(left, right, result);
    }
    LogicalType childType;
    if (!LogicalTypeUtils::tryGetMaxLogicalType(ArrayType::getChildType(left),
            ArrayType::getChildType(right), childType)) {
        return false;
    }
    result = *LogicalType::ARRAY(childType, ArrayType::getNumElements(left));
    return true;
}

static bool tryCombineListArrayTypes(const LogicalType& left, const LogicalType& right,
    LogicalType& result) {
    LogicalType childType;
    if (!LogicalTypeUtils::tryGetMaxLogicalType(ListType::getChildType(left),
            ArrayType::getChildType(right), childType)) {
        return false;
    }
    result = *LogicalType::LIST(childType);
    return true;
}

// If we can match child labels and combine their types, then we can combine
// the struct
static bool tryCombineStructTypes(const LogicalType& left, const LogicalType& right,
    LogicalType& result) {
    auto leftFields = StructType::getFields(left), rightFields = StructType::getFields(right);
    if (leftFields.size() != rightFields.size()) {
        return false;
    }
    std::vector<StructField> newFields;
    for (auto i = 0u; i < leftFields.size(); i++) {
        if (leftFields[i].getName() != rightFields[i].getName()) {
            return false;
        }
        LogicalType combinedType;
        if (LogicalTypeUtils::tryGetMaxLogicalType(leftFields[i].getType(),
                rightFields[i].getType(), combinedType)) {
            newFields.push_back(
                StructField(leftFields[i].getName(), std::make_unique<LogicalType>(combinedType)));
        } else {
            return false;
        }
    }
    result = *LogicalType::STRUCT(std::move(newFields));
    return true;
}

// If we can combine the key and value, then we cna combine the map
static bool tryCombineMapTypes(const LogicalType& left, const LogicalType& right,
    LogicalType& result) {
    auto leftKeyType = MapType::getKeyType(left);
    auto leftValueType = MapType::getValueType(left);
    auto rightKeyType = MapType::getKeyType(right);
    auto rightValueType = MapType::getValueType(right);
    LogicalType resultKeyType, resultValueType;
    if (!LogicalTypeUtils::tryGetMaxLogicalType(leftKeyType, rightKeyType, resultKeyType) ||
        !LogicalTypeUtils::tryGetMaxLogicalType(leftValueType, rightValueType, resultValueType)) {
        return false;
    }
    result = *LogicalType::MAP(std::make_unique<LogicalType>(resultKeyType),
        std::make_unique<LogicalType>(resultValueType));
    return true;
}

/*
// If one of the unions labels is a subset of the other labels, and we can
// combine corresponding labels, then we can combine the union
static bool tryCombineUnionTypes(const LogicalType& left, const LogicalType& right,
    LogicalType& result) {
    auto leftFields = StructType::getFields(left), rightFields = StructType::getFields(right);
    if (leftFields.size() > rightFields.size()) {
        std::swap(leftFields, rightFields);
    }
    std::vector<StructField> newFields;
    for (auto i = 1u, j = 1u; i < leftFields.size(); i++) {
        while (j < rightFields.size() && leftFields[i].getName() != rightFields[j].getName()) {
            j++;
        }
        if (j == rightFields.size()) {
            return false;
        }
        LogicalType combinedType;
        if (!LogicalTypeUtils::tryGetMaxLogicalType(leftFields[i].getType(),
                rightFields[j].getType(), combinedType)) {
            newFields.push_back(
                StructField(leftFields[i].getName(), std::make_unique<LogicalType>(combinedType)));
        }
    }
    result = *LogicalType::UNION(std::move(newFields));
    return true;
}
*/

static LogicalTypeID joinToWiderType(const LogicalTypeID& left, const LogicalTypeID& right) {
    KU_ASSERT(LogicalTypeUtils::isIntegral(left));
    KU_ASSERT(LogicalTypeUtils::isIntegral(right));
    if (PhysicalTypeUtils::getFixedTypeSize(LogicalType::getPhysicalType(left)) >
        PhysicalTypeUtils::getFixedTypeSize(LogicalType::getPhysicalType(right))) {
        return left;
    } else {
        return right;
    }
}

static bool tryUnsignedToSigned(const LogicalTypeID& input, LogicalTypeID& result) {
    switch (input) {
    case LogicalTypeID::UINT8:
        result = LogicalTypeID::INT16;
        break;
    case LogicalTypeID::UINT16:
        result = LogicalTypeID::INT32;
        break;
    case LogicalTypeID::UINT32:
        result = LogicalTypeID::INT64;
        break;
    case LogicalTypeID::UINT64:
        result = LogicalTypeID::INT128;
        break;
    default:
        return false;
    }
    return true;
}

static LogicalTypeID joinDifferentSignIntegrals(const LogicalTypeID& signedType,
    const LogicalTypeID& unsignedType) {
    LogicalTypeID unsignedToSigned;
    if (!tryUnsignedToSigned(unsignedType, unsignedToSigned)) {
        return LogicalTypeID::DOUBLE;
    } else {
        return joinToWiderType(signedType, unsignedToSigned);
    }
}

/*
static uint32_t internalTypeOrder(const LogicalTypeID& type) {
    switch (type) {
    case LogicalTypeID::ANY:
    case LogicalTypeID::RDF_VARIANT:
        return 0;
    case LogicalTypeID::BOOL:
        return 10;
    case LogicalTypeID::UINT8:
        return 11;
    case LogicalTypeID::INT8:
        return 12;
    case LogicalTypeID::UINT16:
        return 13;
    case LogicalTypeID::INT16:
        return 14;
    case LogicalTypeID::UINT32:
        return 15;
    case LogicalTypeID::INT32:
        return 16;
    case LogicalTypeID::UINT64:
        return 17;
    case LogicalTypeID::INT64:
    case LogicalTypeID::SERIAL:
        return 18;
    case LogicalTypeID::INT128:
        return 20;
    case LogicalTypeID::FLOAT:
        return 22;
    case LogicalTypeID::DOUBLE:
        return 23;
    case LogicalTypeID::DATE:
        return 50;
    case LogicalTypeID::TIMESTAMP_SEC:
        return 51;
    case LogicalTypeID::TIMESTAMP_MS:
        return 52;
    case LogicalTypeID::TIMESTAMP:
        return 53;
    case LogicalTypeID::TIMESTAMP_TZ:
        return 54;
    case LogicalTypeID::TIMESTAMP_NS:
        return 55;
    case LogicalTypeID::INTERVAL:
        return 56;
    case LogicalTypeID::STRING:
        return 77;
    case LogicalTypeID::BLOB:
        return 101;
    case LogicalTypeID::UUID:
        return 102;
    case LogicalTypeID::STRUCT:
        return 125;
    case LogicalTypeID::LIST:
    case LogicalTypeID::ARRAY:
        return 126;
    case LogicalTypeID::MAP:
        return 130;
    case LogicalTypeID::UNION:
        return 150;
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::POINTER:
    case LogicalTypeID::INTERNAL_ID:
        return 1000;
    default:
        // LCOV_EXCL_START
        KU_UNREACHABLE;
        // LCOV_EXCL_END
    }
}
*/

static uint32_t internalTimeOrder(const LogicalTypeID& type) {
    switch (type) {
    case LogicalTypeID::DATE:
        return 50;
    case LogicalTypeID::TIMESTAMP_SEC:
        return 51;
    case LogicalTypeID::TIMESTAMP_MS:
        return 52;
    case LogicalTypeID::TIMESTAMP:
        return 53;
    case LogicalTypeID::TIMESTAMP_TZ:
        return 54;
    case LogicalTypeID::TIMESTAMP_NS:
        return 55;
    default:
        return 0; // return 0 if not timestamp
    }
}

static int alwaysCastOrder(const LogicalTypeID& typeID) {
    switch (typeID) {
    case LogicalTypeID::ANY:
        return 0;
    case LogicalTypeID::RDF_VARIANT:
        return 1;
    case LogicalTypeID::STRING:
        return 2;
    default:
        return -1;
    }
}

static bool canAlwaysCast(const LogicalTypeID& typeID) {
    switch (typeID) {
    case LogicalTypeID::ANY:
    case LogicalTypeID::STRING:
    case LogicalTypeID::RDF_VARIANT:
        return true;
    default:
        return false;
    }
}

bool LogicalTypeUtils::tryGetMaxLogicalTypeID(const LogicalTypeID& left, const LogicalTypeID& right,
    LogicalTypeID& result) {
    if (canAlwaysCast(left) && canAlwaysCast(right)) {
        if (alwaysCastOrder(left) > alwaysCastOrder(right)) {
            result = left;
        } else {
            result = right;
        }
        return true;
    }
    if (left == right || canAlwaysCast(left)) {
        result = right;
        return true;
    }
    if (canAlwaysCast(right)) {
        result = left;
        return true;
    }
    auto leftToRight = BuiltInFunctionsUtils::getCastCost(left, right);
    auto rightToLeft = BuiltInFunctionsUtils::getCastCost(right, left);
    if (leftToRight != UNDEFINED_CAST_COST || rightToLeft != UNDEFINED_CAST_COST) {
        if (leftToRight < rightToLeft) {
            result = right;
        } else {
            result = left;
        }
        return true;
    }
    if (isIntegral(left) && isIntegral(right)) {
        if (isUnsigned(left) && !isUnsigned(right)) {
            result = joinDifferentSignIntegrals(right, left);
            return true;
        } else if (isUnsigned(right) && !isUnsigned(left)) {
            result = joinDifferentSignIntegrals(left, right);
            return true;
        }
    }

    // check timestamp combination
    // note: this will become obsolete if implicit casting
    // between timestamps is allowed
    auto leftOrder = internalTimeOrder(left);
    auto rightOrder = internalTimeOrder(right);
    if (leftOrder && rightOrder) {
        if (leftOrder > rightOrder) {
            result = left;
        } else {
            result = right;
        }
        return true;
    }

    return false;
}

static inline bool isSemanticallyNested(LogicalTypeID ID) {
    return LogicalTypeUtils::isNested(ID) && ID != LogicalTypeID::RDF_VARIANT;
}

bool LogicalTypeUtils::tryGetMaxLogicalType(const LogicalType& left, const LogicalType& right,
    LogicalType& result) {
    if (canAlwaysCast(left.typeID) && canAlwaysCast(right.typeID)) {
        if (alwaysCastOrder(left.typeID) > alwaysCastOrder(right.typeID)) {
            result = left;
        } else {
            result = right;
        }
        return true;
    }
    if (left == right || canAlwaysCast(left.typeID)) {
        result = right;
        return true;
    }
    if (canAlwaysCast(right.typeID)) {
        result = left;
        return true;
    }
    if (isSemanticallyNested(left.typeID) || isSemanticallyNested(right.typeID)) {
        if (left.typeID == LogicalTypeID::LIST && right.typeID == LogicalTypeID::ARRAY) {
            return tryCombineListArrayTypes(left, right, result);
        } else if (left.typeID == LogicalTypeID::ARRAY && right.typeID == LogicalTypeID::LIST) {
            return tryCombineListArrayTypes(right, left, result);
        } else if (left.typeID != right.typeID) {
            return false;
        }
        switch (left.typeID) {
        case LogicalTypeID::LIST:
            return tryCombineListTypes(left, right, result);
        case LogicalTypeID::ARRAY:
            return tryCombineArrayTypes(left, right, result);
        case LogicalTypeID::STRUCT:
            return tryCombineStructTypes(left, right, result);
        case LogicalTypeID::MAP:
            return tryCombineMapTypes(left, right, result);
        // LCOV_EXCL_START
        case LogicalTypeID::UNION:
            throw ConversionException("Union casting is not supported");
            // return tryCombineUnionTypes(left, right, result);
        default:
            KU_UNREACHABLE;
            // LCOV_EXCL_END
        }
    }
    LogicalTypeID resultID;
    if (!tryGetMaxLogicalTypeID(left.typeID, right.typeID, resultID)) {
        return false;
    }
    result = LogicalType(resultID);
    return true;
}

bool LogicalTypeUtils::tryGetMaxLogicalType(const std::vector<LogicalType>& types,
    LogicalType& result) {
    LogicalType combinedType(LogicalTypeID::ANY);
    for (auto& type : types) {
        if (!tryGetMaxLogicalType(combinedType, type, combinedType)) {
            return false;
        }
    }
    result = combinedType;
    return true;
}

} // namespace common
} // namespace kuzu
