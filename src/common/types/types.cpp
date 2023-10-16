#include "common/types/types.h"

#include <stdexcept>

#include "common/constants.h"
#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
#include "common/null_buffer.h"
#include "common/ser_deser.h"
#include "common/string_utils.h"
#include "common/types/interval_t.h"
#include "common/types/ku_list.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace common {

std::string PhysicalTypeUtils::physicalTypeToString(PhysicalTypeID physicalType) {
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
    default:
        throw NotImplementedException{"Unrecognized physicalType."};
    }
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
    case PhysicalTypeID::DOUBLE:
        return sizeof(double_t);
    case PhysicalTypeID::FLOAT:
        return sizeof(float_t);
    case PhysicalTypeID::INTERVAL:
        return sizeof(interval_t);
    case PhysicalTypeID::INTERNAL_ID:
        return sizeof(internalID_t);
    default:
        throw NotImplementedException{"PhysicalTypeUtils::getFixedTypeSize."};
    }
}

bool VarListTypeInfo::operator==(const VarListTypeInfo& other) const {
    return *childType == *other.childType;
}

std::unique_ptr<ExtraTypeInfo> VarListTypeInfo::copy() const {
    return std::make_unique<VarListTypeInfo>(childType->copy());
}

std::unique_ptr<ExtraTypeInfo> VarListTypeInfo::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    return std::make_unique<VarListTypeInfo>(LogicalType::deserialize(fileInfo, offset));
}

void VarListTypeInfo::serializeInternal(FileInfo* fileInfo, uint64_t& offset) const {
    childType->serialize(fileInfo, offset);
}

bool FixedListTypeInfo::operator==(const FixedListTypeInfo& other) const {
    return *childType == *other.childType && fixedNumElementsInList == other.fixedNumElementsInList;
}

std::unique_ptr<ExtraTypeInfo> FixedListTypeInfo::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    auto childType = LogicalType::deserialize(fileInfo, offset);
    uint64_t fixedNumElementsInList;
    SerDeser::deserializeValue(fixedNumElementsInList, fileInfo, offset);
    return std::make_unique<FixedListTypeInfo>(std::move(childType), fixedNumElementsInList);
}

std::unique_ptr<ExtraTypeInfo> FixedListTypeInfo::copy() const {
    return std::make_unique<FixedListTypeInfo>(childType->copy(), fixedNumElementsInList);
}

void FixedListTypeInfo::serializeInternal(FileInfo* fileInfo, uint64_t& offset) const {
    VarListTypeInfo::serializeInternal(fileInfo, offset);
    SerDeser::serializeValue(fixedNumElementsInList, fileInfo, offset);
}

bool StructField::operator==(const StructField& other) const {
    return *type == *other.type;
}

void StructField::serialize(FileInfo* fileInfo, uint64_t& offset) const {
    SerDeser::serializeValue(name, fileInfo, offset);
    type->serialize(fileInfo, offset);
}

std::unique_ptr<StructField> StructField::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    std::string name;
    SerDeser::deserializeValue(name, fileInfo, offset);
    auto type = LogicalType::deserialize(fileInfo, offset);
    return std::make_unique<StructField>(std::move(name), std::move(type));
}

std::unique_ptr<StructField> StructField::copy() const {
    return std::make_unique<StructField>(name, type->copy());
}

StructTypeInfo::StructTypeInfo(std::vector<std::unique_ptr<StructField>> fields)
    : fields{std::move(fields)} {
    for (auto i = 0u; i < this->fields.size(); i++) {
        auto fieldName = this->fields[i]->getName();
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
        fields.push_back(std::make_unique<StructField>(fieldName, fieldTypes[i]->copy()));
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

StructField* StructTypeInfo::getStructField(struct_field_idx_t idx) const {
    return fields[idx].get();
}

StructField* StructTypeInfo::getStructField(const std::string& fieldName) const {
    auto idx = getStructFieldIdx(fieldName);
    if (idx == INVALID_STRUCT_FIELD_IDX) {
        throw BinderException("Cannot find field " + fieldName + " in STRUCT.");
    }
    return fields[idx].get();
}

std::vector<LogicalType*> StructTypeInfo::getChildrenTypes() const {
    std::vector<LogicalType*> childrenTypesToReturn{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        childrenTypesToReturn[i] = fields[i]->getType();
    }
    return childrenTypesToReturn;
}

std::vector<std::string> StructTypeInfo::getChildrenNames() const {
    std::vector<std::string> childrenNames{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        childrenNames[i] = fields[i]->getName();
    }
    return childrenNames;
}

std::vector<StructField*> StructTypeInfo::getStructFields() const {
    std::vector<StructField*> structFields{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        structFields[i] = fields[i].get();
    }
    return structFields;
}

bool StructTypeInfo::operator==(const StructTypeInfo& other) const {
    if (fields.size() != other.fields.size()) {
        return false;
    }
    for (auto i = 0u; i < fields.size(); ++i) {
        if (*fields[i] != *other.fields[i]) {
            return false;
        }
    }
    return true;
}

std::unique_ptr<ExtraTypeInfo> StructTypeInfo::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    std::vector<std::unique_ptr<StructField>> fields;
    SerDeser::deserializeVectorOfPtrs(fields, fileInfo, offset);
    return std::make_unique<StructTypeInfo>(std::move(fields));
}

std::unique_ptr<ExtraTypeInfo> StructTypeInfo::copy() const {
    std::vector<std::unique_ptr<StructField>> structFields{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        structFields[i] = fields[i]->copy();
    }
    return std::make_unique<StructTypeInfo>(std::move(structFields));
}

void StructTypeInfo::serializeInternal(FileInfo* fileInfo, uint64_t& offset) const {
    SerDeser::serializeVectorOfPtrs(fields, fileInfo, offset);
}

LogicalType::LogicalType(LogicalTypeID typeID) : typeID{typeID}, extraTypeInfo{nullptr} {
    setPhysicalType();
};
LogicalType::LogicalType(LogicalTypeID typeID, std::unique_ptr<ExtraTypeInfo> extraTypeInfo)
    : typeID{typeID}, extraTypeInfo{std::move(extraTypeInfo)} {
    setPhysicalType();
};

LogicalType::LogicalType(const LogicalType& other) {
    typeID = other.typeID;
    physicalType = other.physicalType;
    if (other.extraTypeInfo != nullptr) {
        extraTypeInfo = other.extraTypeInfo->copy();
    }
}

LogicalType::LogicalType(LogicalType&& other) noexcept
    : typeID{other.typeID}, physicalType{other.physicalType}, extraTypeInfo{
                                                                  std::move(other.extraTypeInfo)} {}

LogicalType& LogicalType::operator=(const LogicalType& other) {
    typeID = other.typeID;
    physicalType = other.physicalType;
    if (other.extraTypeInfo != nullptr) {
        extraTypeInfo = other.extraTypeInfo->copy();
    }
    return *this;
}

bool LogicalType::operator==(const LogicalType& other) const {
    if (typeID != other.typeID) {
        return false;
    }
    switch (other.getPhysicalType()) {
    case PhysicalTypeID::VAR_LIST:
        return *reinterpret_cast<VarListTypeInfo*>(extraTypeInfo.get()) ==
               *reinterpret_cast<VarListTypeInfo*>(other.extraTypeInfo.get());
    case PhysicalTypeID::FIXED_LIST:
        return *reinterpret_cast<FixedListTypeInfo*>(extraTypeInfo.get()) ==
               *reinterpret_cast<FixedListTypeInfo*>(other.extraTypeInfo.get());
    case PhysicalTypeID::STRUCT:
        return *reinterpret_cast<StructTypeInfo*>(extraTypeInfo.get()) ==
               *reinterpret_cast<StructTypeInfo*>(other.extraTypeInfo.get());
    default:
        return true;
    }
}

bool LogicalType::operator!=(const LogicalType& other) const {
    return !((*this) == other);
}

LogicalType& LogicalType::operator=(LogicalType&& other) noexcept {
    typeID = other.typeID;
    physicalType = other.physicalType;
    extraTypeInfo = std::move(other.extraTypeInfo);
    return *this;
}

void LogicalType::serialize(FileInfo* fileInfo, uint64_t& offset) const {
    SerDeser::serializeValue(typeID, fileInfo, offset);
    SerDeser::serializeValue(physicalType, fileInfo, offset);
    switch (physicalType) {
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::FIXED_LIST:
    case PhysicalTypeID::STRUCT:
        extraTypeInfo->serialize(fileInfo, offset);
    default:
        break;
    }
}

std::unique_ptr<LogicalType> LogicalType::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    LogicalTypeID typeID;
    SerDeser::deserializeValue(typeID, fileInfo, offset);
    PhysicalTypeID physicalType;
    SerDeser::deserializeValue(physicalType, fileInfo, offset);
    std::unique_ptr<ExtraTypeInfo> extraTypeInfo;
    switch (physicalType) {
    case PhysicalTypeID::VAR_LIST: {
        extraTypeInfo = VarListTypeInfo::deserialize(fileInfo, offset);
    } break;
    case PhysicalTypeID::FIXED_LIST: {
        extraTypeInfo = FixedListTypeInfo::deserialize(fileInfo, offset);
    } break;
    case PhysicalTypeID::STRUCT: {
        extraTypeInfo = StructTypeInfo::deserialize(fileInfo, offset);
    } break;
    default:
        extraTypeInfo = nullptr;
    }
    return std::make_unique<LogicalType>(typeID, physicalType, std::move(extraTypeInfo));
}

std::unique_ptr<LogicalType> LogicalType::copy() const {
    auto dataType = std::make_unique<LogicalType>(typeID);
    if (extraTypeInfo != nullptr) {
        dataType->extraTypeInfo = extraTypeInfo->copy();
    }
    return dataType;
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

void LogicalType::setPhysicalType() {
    switch (typeID) {
    case LogicalTypeID::ANY: {
        physicalType = PhysicalTypeID::ANY;
    } break;
    case LogicalTypeID::BOOL: {
        physicalType = PhysicalTypeID::BOOL;
    } break;
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        physicalType = PhysicalTypeID::INT64;
    } break;
    case LogicalTypeID::DATE:
    case LogicalTypeID::INT32: {
        physicalType = PhysicalTypeID::INT32;
    } break;
    case LogicalTypeID::INT16: {
        physicalType = PhysicalTypeID::INT16;
    } break;
    case LogicalTypeID::INT8: {
        physicalType = PhysicalTypeID::INT8;
    } break;
    case LogicalTypeID::UINT64: {
        physicalType = PhysicalTypeID::UINT64;
    } break;
    case LogicalTypeID::UINT32: {
        physicalType = PhysicalTypeID::UINT32;
    } break;
    case LogicalTypeID::UINT16: {
        physicalType = PhysicalTypeID::UINT16;
    } break;
    case LogicalTypeID::UINT8: {
        physicalType = PhysicalTypeID::UINT8;
    } break;
    case LogicalTypeID::DOUBLE: {
        physicalType = PhysicalTypeID::DOUBLE;
    } break;
    case LogicalTypeID::FLOAT: {
        physicalType = PhysicalTypeID::FLOAT;
    } break;
    case LogicalTypeID::INTERVAL: {
        physicalType = PhysicalTypeID::INTERVAL;
    } break;
    case LogicalTypeID::FIXED_LIST: {
        physicalType = PhysicalTypeID::FIXED_LIST;
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        physicalType = PhysicalTypeID::INTERNAL_ID;
    } break;
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        physicalType = PhysicalTypeID::STRING;
    } break;
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        physicalType = PhysicalTypeID::VAR_LIST;
    } break;
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::UNION:
    case LogicalTypeID::STRUCT: {
        physicalType = PhysicalTypeID::STRUCT;
    } break;
    case LogicalTypeID::ARROW_COLUMN: {
        physicalType = PhysicalTypeID::ARROW_COLUMN;
    } break;
    default:
        throw NotImplementedException{"LogicalType::setPhysicalType()."};
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
    dataType.setPhysicalType();
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
    } else if ("DOUBLE" == upperDataTypeIDString) {
        return LogicalTypeID::DOUBLE;
    } else if ("FLOAT" == upperDataTypeIDString) {
        return LogicalTypeID::FLOAT;
    } else if ("BOOLEAN" == upperDataTypeIDString) {
        return LogicalTypeID::BOOL;
    } else if ("BYTEA" == upperDataTypeIDString || "BLOB" == upperDataTypeIDString) {
        return LogicalTypeID::BLOB;
    } else if ("STRING" == upperDataTypeIDString) {
        return LogicalTypeID::STRING;
    } else if ("DATE" == upperDataTypeIDString) {
        return LogicalTypeID::DATE;
    } else if ("TIMESTAMP" == upperDataTypeIDString) {
        return LogicalTypeID::TIMESTAMP;
    } else if ("INTERVAL" == upperDataTypeIDString) {
        return LogicalTypeID::INTERVAL;
    } else if ("SERIAL" == upperDataTypeIDString) {
        return LogicalTypeID::SERIAL;
    } else {
        throw NotImplementedException("Cannot parse dataTypeID: " + dataTypeIDString);
    }
}

std::string LogicalTypeUtils::dataTypeToString(const LogicalType& dataType) {
    switch (dataType.typeID) {
    case LogicalTypeID::MAP: {
        auto structType = VarListType::getChildType(&dataType);
        auto fieldTypes = StructType::getFieldTypes(structType);
        return "MAP(" + dataTypeToString(*fieldTypes[0]) + ": " + dataTypeToString(*fieldTypes[1]) +
               ")";
    }
    case LogicalTypeID::VAR_LIST: {
        auto varListTypeInfo = reinterpret_cast<VarListTypeInfo*>(dataType.extraTypeInfo.get());
        return dataTypeToString(*varListTypeInfo->getChildType()) + "[]";
    }
    case LogicalTypeID::FIXED_LIST: {
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(dataType.extraTypeInfo.get());
        return dataTypeToString(*fixedListTypeInfo->getChildType()) + "[" +
               std::to_string(fixedListTypeInfo->getNumElementsInList()) + "]";
    }
    case LogicalTypeID::UNION: {
        auto unionTypeInfo = reinterpret_cast<StructTypeInfo*>(dataType.extraTypeInfo.get());
        std::string dataTypeStr = dataTypeToString(dataType.typeID) + "(";
        auto numFields = unionTypeInfo->getChildrenTypes().size();
        auto fieldNames = unionTypeInfo->getChildrenNames();
        for (auto i = 1u; i < numFields; i++) {
            dataTypeStr += fieldNames[i] + ":";
            dataTypeStr += dataTypeToString(*unionTypeInfo->getChildrenTypes()[i]);
            dataTypeStr += (i == numFields - 1 ? ")" : ", ");
        }
        return dataTypeStr;
    }
    case LogicalTypeID::STRUCT: {
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(dataType.extraTypeInfo.get());
        std::string dataTypeStr = dataTypeToString(dataType.typeID) + "(";
        auto numFields = structTypeInfo->getChildrenTypes().size();
        auto fieldNames = structTypeInfo->getChildrenNames();
        for (auto i = 0u; i < numFields - 1; i++) {
            dataTypeStr += fieldNames[i] + ":";
            dataTypeStr += dataTypeToString(*structTypeInfo->getChildrenTypes()[i]);
            dataTypeStr += ", ";
        }
        dataTypeStr += fieldNames[numFields - 1] + ":";
        dataTypeStr += dataTypeToString(*structTypeInfo->getChildrenTypes()[numFields - 1]);
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
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING:
    case LogicalTypeID::SERIAL:
        return dataTypeToString(dataType.typeID);
    default:
        throw NotImplementedException("LogicalTypeUtils::dataTypeToString.");
    }
}

std::string LogicalTypeUtils::dataTypeToString(LogicalTypeID dataTypeID) {
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
    case LogicalTypeID::DOUBLE:
        return "DOUBLE";
    case LogicalTypeID::FLOAT:
        return "FLOAT";
    case LogicalTypeID::DATE:
        return "DATE";
    case LogicalTypeID::TIMESTAMP:
        return "TIMESTAMP";
    case LogicalTypeID::INTERVAL:
        return "INTERVAL";
    case LogicalTypeID::BLOB:
        return "BLOB";
    case LogicalTypeID::STRING:
        return "STRING";
    case LogicalTypeID::VAR_LIST:
        return "VAR_LIST";
    case LogicalTypeID::FIXED_LIST:
        return "FIXED_LIST";
    case LogicalTypeID::STRUCT:
        return "STRUCT";
    case LogicalTypeID::SERIAL:
        return "SERIAL";
    case LogicalTypeID::MAP:
        return "MAP";
    case LogicalTypeID::UNION:
        return "UNION";
    default:
        throw NotImplementedException("LogicalTypeUtils::dataTypeToString.");
    }
}

std::string LogicalTypeUtils::dataTypesToString(const std::vector<LogicalType>& dataTypes) {
    std::vector<LogicalTypeID> dataTypeIDs;
    for (auto& dataType : dataTypes) {
        dataTypeIDs.push_back(dataType.typeID);
    }
    return dataTypesToString(dataTypeIDs);
}

std::string LogicalTypeUtils::dataTypesToString(const std::vector<LogicalTypeID>& dataTypeIDs) {
    if (dataTypeIDs.empty()) {
        return {""};
    }
    std::string result = "(" + LogicalTypeUtils::dataTypeToString(dataTypeIDs[0]);
    for (auto i = 1u; i < dataTypeIDs.size(); ++i) {
        result += "," + LogicalTypeUtils::dataTypeToString(dataTypeIDs[i]);
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
               FixedListType::getNumElementsInList(&type);
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
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::SERIAL:
        return true;
    default:
        return false;
    }
}

bool LogicalTypeUtils::isNested(const LogicalType& dataType) {
    switch (dataType.typeID) {
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::UNION:
    case LogicalTypeID::MAP:
        return true;
    default:
        return false;
    }
}

std::vector<LogicalType> LogicalTypeUtils::getAllValidComparableLogicalTypes() {
    return std::vector<LogicalType>{LogicalType{LogicalTypeID::BOOL},
        LogicalType{LogicalTypeID::INT64}, LogicalType{LogicalTypeID::INT32},
        LogicalType{LogicalTypeID::INT16}, LogicalType{LogicalTypeID::INT8},
        LogicalType{LogicalTypeID::UINT64}, LogicalType{LogicalTypeID::UINT32},
        LogicalType{LogicalTypeID::UINT16}, LogicalType{LogicalTypeID::UINT8},
        LogicalType{LogicalTypeID::DOUBLE}, LogicalType{LogicalTypeID::FLOAT},
        LogicalType{LogicalTypeID::DATE}, LogicalType{LogicalTypeID::TIMESTAMP},
        LogicalType{LogicalTypeID::INTERVAL}, LogicalType{LogicalTypeID::BLOB},
        LogicalType{LogicalTypeID::STRING}, LogicalType{LogicalTypeID::SERIAL}};
}

std::vector<LogicalTypeID> LogicalTypeUtils::getNumericalLogicalTypeIDs() {
    return std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::INT32,
        LogicalTypeID::INT16, LogicalTypeID::INT8, LogicalTypeID::UINT64, LogicalTypeID::UINT32,
        LogicalTypeID::UINT16, LogicalTypeID::UINT8, LogicalTypeID::DOUBLE, LogicalTypeID::FLOAT,
        LogicalTypeID::SERIAL};
}

std::vector<LogicalTypeID> LogicalTypeUtils::getIntegerLogicalTypeIDs() {
    return std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::INT32,
        LogicalTypeID::INT16, LogicalTypeID::INT8, LogicalTypeID::SERIAL};
}

std::vector<LogicalType> LogicalTypeUtils::getAllValidLogicTypes() {
    // TODO(Ziyi): Add FIX_LIST,STRUCT,MAP type to allValidTypeID when we support functions on
    // FIXED_LIST,STRUCT,MAP.
    return std::vector<LogicalType>{LogicalType{LogicalTypeID::INTERNAL_ID},
        LogicalType{LogicalTypeID::BOOL}, LogicalType{LogicalTypeID::INT64},
        LogicalType{LogicalTypeID::INT32}, LogicalType{LogicalTypeID::INT16},
        LogicalType{LogicalTypeID::INT8}, LogicalType{LogicalTypeID::UINT64},
        LogicalType{LogicalTypeID::UINT32}, LogicalType{LogicalTypeID::UINT16},
        LogicalType{LogicalTypeID::UINT8}, LogicalType{LogicalTypeID::DOUBLE},
        LogicalType{LogicalTypeID::STRING}, LogicalType{LogicalTypeID::BLOB},
        LogicalType{LogicalTypeID::DATE}, LogicalType{LogicalTypeID::TIMESTAMP},
        LogicalType{LogicalTypeID::INTERVAL}, LogicalType{LogicalTypeID::VAR_LIST},
        LogicalType{LogicalTypeID::FLOAT}, LogicalType{LogicalTypeID::SERIAL},
        LogicalType{LogicalTypeID::NODE}, LogicalType{LogicalTypeID::REL}};
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
        }
        curPos++;
    }
    structFieldsStr.push_back(
        StringUtils::ltrim(structTypeStr.substr(startPos, curPos - startPos)));
    return structFieldsStr;
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseVarListType(const std::string& trimmedStr) {
    return std::make_unique<LogicalType>(LogicalTypeID::VAR_LIST,
        std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(
            dataTypeFromString(trimmedStr.substr(0, trimmedStr.size() - 2)))));
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseFixedListType(const std::string& trimmedStr) {
    auto leftBracketPos = trimmedStr.find('[');
    auto rightBracketPos = trimmedStr.find(']');
    auto childType =
        std::make_unique<LogicalType>(dataTypeFromString(trimmedStr.substr(0, leftBracketPos)));
    auto fixedNumElementsInList = std::strtoll(
        trimmedStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1).c_str(),
        nullptr, 0 /* base */);
    return std::make_unique<LogicalType>(LogicalTypeID::FIXED_LIST,
        std::make_unique<FixedListTypeInfo>(std::move(childType), fixedNumElementsInList));
}

std::vector<std::unique_ptr<StructField>> LogicalTypeUtils::parseStructTypeInfo(
    const std::string& structTypeStr) {
    auto leftBracketPos = structTypeStr.find('(');
    auto rightBracketPos = structTypeStr.find_last_of(')');
    if (leftBracketPos == std::string::npos || rightBracketPos == std::string::npos) {
        throw Exception("Cannot parse struct type: " + structTypeStr);
    }
    // Remove the leading and trailing brackets.
    auto structFieldsStr =
        structTypeStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
    std::vector<std::unique_ptr<StructField>> structFields;
    auto structFieldStrs = parseStructFields(structFieldsStr);
    for (auto& structFieldStr : structFieldStrs) {
        auto pos = structFieldStr.find(' ');
        auto fieldName = structFieldStr.substr(0, pos);
        auto fieldTypeString = structFieldStr.substr(pos + 1);
        structFields.emplace_back(std::make_unique<StructField>(
            fieldName, std::make_unique<LogicalType>(dataTypeFromString(fieldTypeString))));
    }
    return structFields;
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseStructType(const std::string& trimmedStr) {
    return std::make_unique<LogicalType>(
        LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(parseStructTypeInfo(trimmedStr)));
}

std::unique_ptr<LogicalType> MapType::createMapType(
    std::unique_ptr<LogicalType> keyType, std::unique_ptr<LogicalType> valueType) {
    std::vector<std::unique_ptr<StructField>> structFields;
    structFields.push_back(
        std::make_unique<StructField>(InternalKeyword::MAP_KEY, std::move(keyType)));
    structFields.push_back(
        std::make_unique<StructField>(InternalKeyword::MAP_VALUE, std::move(valueType)));
    auto childType = std::make_unique<LogicalType>(
        LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(structFields)));
    return std::make_unique<LogicalType>(
        LogicalTypeID::MAP, std::make_unique<VarListTypeInfo>(std::move(childType)));
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseMapType(const std::string& trimmedStr) {
    auto leftBracketPos = trimmedStr.find('(');
    auto rightBracketPos = trimmedStr.find_last_of(')');
    if (leftBracketPos == std::string::npos || rightBracketPos == std::string::npos) {
        throw Exception("Cannot parse map type: " + trimmedStr);
    }
    auto mapTypeStr = trimmedStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
    auto keyValueTypes = StringUtils::splitComma(mapTypeStr);
    return MapType::createMapType(
        std::make_unique<LogicalType>(dataTypeFromString(keyValueTypes[0])),
        std::make_unique<LogicalType>(dataTypeFromString(keyValueTypes[1])));
}

std::unique_ptr<LogicalType> LogicalTypeUtils::parseUnionType(const std::string& trimmedStr) {
    auto unionFields = parseStructTypeInfo(trimmedStr);
    auto unionTagField = std::make_unique<StructField>(
        UnionType::TAG_FIELD_NAME, std::make_unique<LogicalType>(UnionType::TAG_FIELD_TYPE));
    unionFields.insert(unionFields.begin(), std::move(unionTagField));
    return std::make_unique<LogicalType>(
        LogicalTypeID::UNION, std::make_unique<StructTypeInfo>(std::move(unionFields)));
}

} // namespace common
} // namespace kuzu
