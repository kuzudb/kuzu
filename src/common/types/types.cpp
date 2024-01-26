#include "common/types/types.h"

#include "common/cast.h"
#include "common/constants.h"
#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
#include "common/null_buffer.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"
#include "common/types/int128_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_list.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace common {

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
    case PhysicalTypeID::FIXED_LIST:
        return "FIXED_LIST";
    case PhysicalTypeID::INTERNAL_ID:
        return "INTERNAL_ID";
    case PhysicalTypeID::STRING:
        return "STRING";
    case PhysicalTypeID::STRUCT:
        return "STRUCT";
    case PhysicalTypeID::VAR_LIST:
        return "VAR_LIST";
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

bool VarListTypeInfo::operator==(const VarListTypeInfo& other) const {
    return *childType == *other.childType;
}

std::unique_ptr<ExtraTypeInfo> VarListTypeInfo::copy() const {
    return std::make_unique<VarListTypeInfo>(childType->copy());
}

std::unique_ptr<ExtraTypeInfo> VarListTypeInfo::deserialize(Deserializer& deserializer) {
    return std::make_unique<VarListTypeInfo>(LogicalType::deserialize(deserializer));
}

void VarListTypeInfo::serializeInternal(Serializer& serializer) const {
    childType->serialize(serializer);
}

bool FixedListTypeInfo::operator==(const FixedListTypeInfo& other) const {
    return *childType == *other.childType && fixedNumElementsInList == other.fixedNumElementsInList;
}

std::unique_ptr<ExtraTypeInfo> FixedListTypeInfo::deserialize(Deserializer& deserializer) {
    auto childType = LogicalType::deserialize(deserializer);
    uint64_t fixedNumElementsInList;
    deserializer.deserializeValue(fixedNumElementsInList);
    return std::make_unique<FixedListTypeInfo>(std::move(childType), fixedNumElementsInList);
}

std::unique_ptr<ExtraTypeInfo> FixedListTypeInfo::copy() const {
    return std::make_unique<FixedListTypeInfo>(childType->copy(), fixedNumElementsInList);
}

void FixedListTypeInfo::serializeInternal(Serializer& serializer) const {
    VarListTypeInfo::serializeInternal(serializer);
    serializer.serializeValue(fixedNumElementsInList);
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

const StructField* StructTypeInfo::getStructField(struct_field_idx_t idx) const {
    return &fields[idx];
}

const StructField* StructTypeInfo::getStructField(const std::string& fieldName) const {
    auto idx = getStructFieldIdx(fieldName);
    if (idx == INVALID_STRUCT_FIELD_IDX) {
        throw BinderException("Cannot find field " + fieldName + " in STRUCT.");
    }
    return &fields[idx];
}

LogicalType* StructTypeInfo::getChildType(kuzu::common::struct_field_idx_t idx) const {
    return fields[idx].getType();
}

std::vector<LogicalType*> StructTypeInfo::getChildrenTypes() const {
    std::vector<LogicalType*> childrenTypesToReturn{fields.size()};
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

std::vector<const StructField*> StructTypeInfo::getStructFields() const {
    std::vector<const StructField*> structFields{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        structFields[i] = &fields[i];
    }
    return structFields;
}

bool StructTypeInfo::operator==(const StructTypeInfo& other) const {
    if (fields.size() != other.fields.size()) {
        return false;
    }
    for (auto i = 0u; i < fields.size(); ++i) {
        if (fields[i] != other.fields[i]) {
            return false;
        }
    }
    return true;
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

LogicalType::LogicalType(LogicalTypeID typeID) : typeID{typeID}, extraTypeInfo{nullptr} {
    physicalType = getPhysicalType(typeID);
    // Complex types should not use this constructor as they need extra type information
    KU_ASSERT(physicalType != PhysicalTypeID::VAR_LIST);
    KU_ASSERT(physicalType != PhysicalTypeID::FIXED_LIST);
    // Node/Rel types are exempted due to some complex code in bind_graph_pattern.cpp
    KU_ASSERT(physicalType != PhysicalTypeID::STRUCT || typeID == LogicalTypeID::NODE ||
              typeID == LogicalTypeID::REL || typeID == LogicalTypeID::RECURSIVE_REL);
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

LogicalType& LogicalType::operator=(const LogicalType& other) {
    // Reuse the copy constructor and move assignment operator.
    LogicalType copy(other);
    return *this = std::move(copy);
}

bool LogicalType::operator==(const LogicalType& other) const {
    if (typeID != other.typeID) {
        return false;
    }
    switch (other.getPhysicalType()) {
    case PhysicalTypeID::VAR_LIST:
        return *ku_dynamic_cast<ExtraTypeInfo*, VarListTypeInfo*>(extraTypeInfo.get()) ==
               *ku_dynamic_cast<ExtraTypeInfo*, VarListTypeInfo*>(other.extraTypeInfo.get());
    case PhysicalTypeID::FIXED_LIST:
        return *ku_dynamic_cast<ExtraTypeInfo*, FixedListTypeInfo*>(extraTypeInfo.get()) ==
               *ku_dynamic_cast<ExtraTypeInfo*, FixedListTypeInfo*>(other.extraTypeInfo.get());
    case PhysicalTypeID::STRUCT:
        return *ku_dynamic_cast<ExtraTypeInfo*, StructTypeInfo*>(extraTypeInfo.get()) ==
               *ku_dynamic_cast<ExtraTypeInfo*, StructTypeInfo*>(other.extraTypeInfo.get());
    default:
        return true;
    }
}

bool LogicalType::operator!=(const LogicalType& other) const {
    return !((*this) == other);
}

std::string LogicalType::toString() const {
    switch (typeID) {
    case LogicalTypeID::MAP: {
        auto structType =
            ku_dynamic_cast<ExtraTypeInfo*, VarListTypeInfo*>(extraTypeInfo.get())->getChildType();
        auto fieldTypes = StructType::getFieldTypes(structType);
        return "MAP(" + fieldTypes[0]->toString() + ": " + fieldTypes[1]->toString() + ")";
    }
    case LogicalTypeID::VAR_LIST: {
        auto varListTypeInfo =
            ku_dynamic_cast<ExtraTypeInfo*, VarListTypeInfo*>(extraTypeInfo.get());
        return varListTypeInfo->getChildType()->toString() + "[]";
    }
    case LogicalTypeID::FIXED_LIST: {
        auto fixedListTypeInfo =
            ku_dynamic_cast<ExtraTypeInfo*, FixedListTypeInfo*>(extraTypeInfo.get());
        return fixedListTypeInfo->getChildType()->toString() + "[" +
               std::to_string(fixedListTypeInfo->getNumValuesInList()) + "]";
    }
    case LogicalTypeID::UNION: {
        auto unionTypeInfo = ku_dynamic_cast<ExtraTypeInfo*, StructTypeInfo*>(extraTypeInfo.get());
        std::string dataTypeStr = LogicalTypeUtils::toString(typeID) + "(";
        auto numFields = unionTypeInfo->getChildrenTypes().size();
        auto fieldNames = unionTypeInfo->getChildrenNames();
        for (auto i = 1u; i < numFields; i++) {
            dataTypeStr += fieldNames[i] + ":";
            dataTypeStr += unionTypeInfo->getChildType(i)->toString();
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
            dataTypeStr += fieldNames[i] + ":";
            dataTypeStr += structTypeInfo->getChildType(i)->toString();
            dataTypeStr += ", ";
        }
        dataTypeStr += fieldNames[numFields - 1] + ":";
        dataTypeStr += structTypeInfo->getChildType(numFields - 1)->toString();
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

void LogicalType::serialize(Serializer& serializer) const {
    serializer.serializeValue(typeID);
    serializer.serializeValue(physicalType);
    switch (physicalType) {
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::FIXED_LIST:
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
    case PhysicalTypeID::VAR_LIST: {
        extraTypeInfo = VarListTypeInfo::deserialize(deserializer);
    } break;
    case PhysicalTypeID::FIXED_LIST: {
        extraTypeInfo = FixedListTypeInfo::deserialize(deserializer);
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
    typesCopy.reserve(types.size());
    for (auto& type : types) {
        typesCopy.push_back(type->copy());
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
    case LogicalTypeID::FIXED_LIST: {
        return PhysicalTypeID::FIXED_LIST;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        return PhysicalTypeID::INTERNAL_ID;
    } break;
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        return PhysicalTypeID::STRING;
    } break;
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        return PhysicalTypeID::VAR_LIST;
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

LogicalType LogicalTypeUtils::dataTypeFromString(const std::string& dataTypeString) {
    LogicalType dataType;
    auto trimmedStr = StringUtils::ltrim(StringUtils::rtrim(dataTypeString));
    auto upperDataTypeString = StringUtils::getUpper(trimmedStr);
    if (upperDataTypeString.ends_with("[]")) {
        dataType = *parseVarListType(trimmedStr);
    } else if (upperDataTypeString.ends_with("]")) {
        dataType = *parseFixedListType(trimmedStr);
    } else if (upperDataTypeString.starts_with("STRUCT")) {
        dataType = *parseStructType(trimmedStr);
    } else if (upperDataTypeString.starts_with("MAP")) {
        dataType = *parseMapType(trimmedStr);
    } else if (upperDataTypeString.starts_with("UNION")) {
        dataType = *parseUnionType(trimmedStr);
    } else {
        dataType.typeID = dataTypeIDFromString(upperDataTypeString);
    }
    dataType.physicalType = LogicalType::getPhysicalType(dataType.typeID);
    return dataType;
}

LogicalTypeID LogicalTypeUtils::dataTypeIDFromString(const std::string& dataTypeIDString) {
    auto upperDataTypeIDString = StringUtils::getUpper(dataTypeIDString);
    if ("INTERNAL_ID" == upperDataTypeIDString) {
        return LogicalTypeID::INTERNAL_ID;
    } else if ("INT64" == upperDataTypeIDString) {
        return LogicalTypeID::INT64;
    } else if ("INT32" == upperDataTypeIDString || "INT" == upperDataTypeIDString) {
        return LogicalTypeID::INT32;
    } else if ("INT16" == upperDataTypeIDString) {
        return LogicalTypeID::INT16;
    } else if ("INT8" == upperDataTypeIDString) {
        return LogicalTypeID::INT8;
    } else if ("UINT64" == upperDataTypeIDString) {
        return LogicalTypeID::UINT64;
    } else if ("UINT32" == upperDataTypeIDString) {
        return LogicalTypeID::UINT32;
    } else if ("UINT16" == upperDataTypeIDString) {
        return LogicalTypeID::UINT16;
    } else if ("UINT8" == upperDataTypeIDString) {
        return LogicalTypeID::UINT8;
    } else if ("INT128" == upperDataTypeIDString) {
        return LogicalTypeID::INT128;
    } else if ("DOUBLE" == upperDataTypeIDString) {
        return LogicalTypeID::DOUBLE;
    } else if ("FLOAT" == upperDataTypeIDString) {
        return LogicalTypeID::FLOAT;
    } else if ("BOOLEAN" == upperDataTypeIDString || "BOOL" == upperDataTypeIDString) {
        return LogicalTypeID::BOOL;
    } else if ("BYTEA" == upperDataTypeIDString || "BLOB" == upperDataTypeIDString) {
        return LogicalTypeID::BLOB;
    } else if ("UUID" == upperDataTypeIDString) {
        return LogicalTypeID::UUID;
    } else if ("STRING" == upperDataTypeIDString) {
        return LogicalTypeID::STRING;
    } else if ("DATE" == upperDataTypeIDString) {
        return LogicalTypeID::DATE;
    } else if ("TIMESTAMP" == upperDataTypeIDString) {
        return LogicalTypeID::TIMESTAMP;
    } else if ("TIMESTAMP_NS" == upperDataTypeIDString) {
        return LogicalTypeID::TIMESTAMP_NS;
    } else if ("TIMESTAMP_MS" == upperDataTypeIDString) {
        return LogicalTypeID::TIMESTAMP_MS;
    } else if ("TIMESTAMP_SEC" == upperDataTypeIDString || "TIMESTAMP_S" == upperDataTypeIDString) {
        return LogicalTypeID::TIMESTAMP_SEC;
    } else if ("TIMESTAMP_TZ" == upperDataTypeIDString) {
        return LogicalTypeID::TIMESTAMP_TZ;
    } else if ("INTERVAL" == upperDataTypeIDString) {
        return LogicalTypeID::INTERVAL;
    } else if ("SERIAL" == upperDataTypeIDString) {
        return LogicalTypeID::SERIAL;
    } else {
        throw NotImplementedException("Cannot parse dataTypeID: " + dataTypeIDString);
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
    case LogicalTypeID::VAR_LIST:
        return "VAR_LIST";
    case LogicalTypeID::FIXED_LIST:
        return "FIXED_LIST";
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

std::string LogicalTypeUtils::toString(const std::vector<LogicalType>& dataTypes) {
    std::vector<LogicalTypeID> dataTypeIDs;
    dataTypeIDs.reserve(dataTypes.size());
    for (auto& dataType : dataTypes) {
        dataTypeIDs.push_back(dataType.typeID);
    }
    return toString(dataTypeIDs);
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
    case PhysicalTypeID::FIXED_LIST: {
        return getRowLayoutSize(*FixedListType::getChildType(&type)) *
               FixedListType::getNumValuesInList(&type);
    }
    case PhysicalTypeID::VAR_LIST: {
        return sizeof(ku_list_t);
    }
    case PhysicalTypeID::STRUCT: {
        uint32_t size = 0;
        auto fieldsTypes = StructType::getFieldTypes(&type);
        for (auto fieldType : fieldsTypes) {
            size += getRowLayoutSize(*fieldType);
        }
        size += NullBuffer::getNumBytesForNullValues(fieldsTypes.size());
        return size;
    }
    default:
        return PhysicalTypeUtils::getFixedTypeSize(type.getPhysicalType());
    }
}

bool LogicalTypeUtils::isNumerical(const LogicalType& dataType) {
    switch (dataType.typeID) {
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
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
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

std::vector<LogicalTypeID> LogicalTypeUtils::getNumericalLogicalTypeIDs() {
    return std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::INT32,
        LogicalTypeID::INT16, LogicalTypeID::INT8, LogicalTypeID::UINT64, LogicalTypeID::UINT32,
        LogicalTypeID::UINT16, LogicalTypeID::UINT8, LogicalTypeID::INT128, LogicalTypeID::DOUBLE,
        LogicalTypeID::FLOAT, LogicalTypeID::SERIAL};
}

std::vector<LogicalTypeID> LogicalTypeUtils::getIntegerLogicalTypeIDs() {
    return std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::INT32,
        LogicalTypeID::INT16, LogicalTypeID::INT8, LogicalTypeID::SERIAL};
}

std::vector<LogicalTypeID> LogicalTypeUtils::getAllValidLogicTypes() {
    return std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID, LogicalTypeID::BOOL,
        LogicalTypeID::INT64, LogicalTypeID::INT32, LogicalTypeID::INT16, LogicalTypeID::INT8,
        LogicalTypeID::UINT64, LogicalTypeID::UINT32, LogicalTypeID::UINT16, LogicalTypeID::UINT8,
        LogicalTypeID::INT128, LogicalTypeID::DOUBLE, LogicalTypeID::STRING, LogicalTypeID::BLOB,
        LogicalTypeID::UUID, LogicalTypeID::DATE, LogicalTypeID::TIMESTAMP,
        LogicalTypeID::TIMESTAMP_NS, LogicalTypeID::TIMESTAMP_MS, LogicalTypeID::TIMESTAMP_SEC,
        LogicalTypeID::TIMESTAMP_TZ, LogicalTypeID::INTERVAL, LogicalTypeID::VAR_LIST,
        LogicalTypeID::FIXED_LIST, LogicalTypeID::MAP, LogicalTypeID::FLOAT, LogicalTypeID::SERIAL,
        LogicalTypeID::NODE, LogicalTypeID::REL, LogicalTypeID::STRUCT, LogicalTypeID::UNION,
        LogicalTypeID::RDF_VARIANT};
}

std::vector<std::string> LogicalTypeUtils::parseStructFields(const std::string& structTypeStr) {
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

std::unique_ptr<LogicalType> LogicalTypeUtils::parseVarListType(const std::string& trimmedStr) {
    return LogicalType::VAR_LIST(dataTypeFromString(trimmedStr.substr(0, trimmedStr.size() - 2)));
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseFixedListType(const std::string& trimmedStr) {
    auto leftBracketPos = trimmedStr.find('[');
    auto rightBracketPos = trimmedStr.find(']');
    auto childType =
        std::make_unique<LogicalType>(dataTypeFromString(trimmedStr.substr(0, leftBracketPos)));
    auto fixedNumElementsInList = std::strtoll(
        trimmedStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1).c_str(),
        nullptr, 0 /* base */);
    return LogicalType::FIXED_LIST(std::move(childType), fixedNumElementsInList);
}

std::vector<StructField> LogicalTypeUtils::parseStructTypeInfo(const std::string& structTypeStr) {
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
        structFields.emplace_back(
            fieldName, std::make_unique<LogicalType>(dataTypeFromString(fieldTypeString)));
    }
    return structFields;
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseStructType(const std::string& trimmedStr) {
    return LogicalType::STRUCT(parseStructTypeInfo(trimmedStr));
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseMapType(const std::string& trimmedStr) {
    auto leftBracketPos = trimmedStr.find('(');
    auto rightBracketPos = trimmedStr.find_last_of(')');
    if (leftBracketPos == std::string::npos || rightBracketPos == std::string::npos) {
        throw Exception("Cannot parse map type: " + trimmedStr);
    }
    auto mapTypeStr = trimmedStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
    auto keyValueTypes = StringUtils::splitComma(mapTypeStr);
    return LogicalType::MAP(
        dataTypeFromString(keyValueTypes[0]), dataTypeFromString(keyValueTypes[1]));
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseUnionType(const std::string& trimmedStr) {
    auto unionFields = parseStructTypeInfo(trimmedStr);
    auto unionTagField = StructField(
        UnionType::TAG_FIELD_NAME, std::make_unique<LogicalType>(UnionType::TAG_FIELD_TYPE));
    unionFields.insert(unionFields.begin(), std::move(unionTagField));
    return LogicalType::UNION(std::move(unionFields));
}

std::unique_ptr<LogicalType> LogicalType::STRUCT(std::vector<StructField>&& fields) {
    return std::unique_ptr<LogicalType>(new LogicalType(
        LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(fields))));
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

std::unique_ptr<LogicalType> LogicalType::VAR_LIST(std::unique_ptr<LogicalType> childType) {
    return std::unique_ptr<LogicalType>(new LogicalType(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(std::move(childType))));
}

std::unique_ptr<LogicalType> LogicalType::MAP(
    std::unique_ptr<LogicalType> keyType, std::unique_ptr<LogicalType> valueType) {
    std::vector<StructField> structFields;
    structFields.emplace_back(InternalKeyword::MAP_KEY, std::move(keyType));
    structFields.emplace_back(InternalKeyword::MAP_VALUE, std::move(valueType));
    auto mapStructType = LogicalType::STRUCT(std::move(structFields));
    return std::unique_ptr<LogicalType>(new LogicalType(
        LogicalTypeID::MAP, std::make_unique<VarListTypeInfo>(std::move(mapStructType))));
}

std::unique_ptr<LogicalType> LogicalType::FIXED_LIST(
    std::unique_ptr<LogicalType> childType, uint64_t fixedNumElementsInList) {
    return std::unique_ptr<LogicalType>(new LogicalType(LogicalTypeID::FIXED_LIST,
        std::make_unique<FixedListTypeInfo>(std::move(childType), fixedNumElementsInList)));
}

} // namespace common
} // namespace kuzu
