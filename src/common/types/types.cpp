#include "common/types/types.h"

#include <stdexcept>

#include "common/exception.h"
#include "common/null_buffer.h"
#include "common/ser_deser.h"
#include "common/string_utils.h"
#include "common/types/types_include.h"

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
        throw common::NotImplementedException{"Unrecognized physicalType."};
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

bool VarListTypeInfo::operator==(const kuzu::common::VarListTypeInfo& other) const {
    return *childType == *other.childType;
}

std::unique_ptr<ExtraTypeInfo> VarListTypeInfo::copy() const {
    return std::make_unique<VarListTypeInfo>(childType->copy());
}

bool FixedListTypeInfo::operator==(const kuzu::common::FixedListTypeInfo& other) const {
    return *childType == *other.childType && fixedNumElementsInList == other.fixedNumElementsInList;
}

std::unique_ptr<ExtraTypeInfo> FixedListTypeInfo::copy() const {
    return std::make_unique<FixedListTypeInfo>(childType->copy(), fixedNumElementsInList);
}

StructField::StructField(std::string name, std::unique_ptr<LogicalType> type)
    : name{std::move(name)}, type{std::move(type)} {
    // Note: struct field name is case-insensitive.
    StringUtils::toUpper(this->name);
}

bool StructField::operator==(const kuzu::common::StructField& other) const {
    return *type == *other.type;
}

std::unique_ptr<StructField> StructField::copy() const {
    return std::make_unique<StructField>(name, type->copy());
}

StructTypeInfo::StructTypeInfo(std::vector<std::unique_ptr<StructField>> fields)
    : fields{std::move(fields)} {
    for (auto i = 0u; i < this->fields.size(); i++) {
        fieldNameToIdxMap.emplace(this->fields[i]->getName(), i);
    }
}

struct_field_idx_t StructTypeInfo::getStructFieldIdx(std::string fieldName) const {
    StringUtils::toUpper(fieldName);
    if (fieldNameToIdxMap.contains(fieldName)) {
        return fieldNameToIdxMap.at(fieldName);
    }
    return INVALID_STRUCT_FIELD_IDX;
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

bool StructTypeInfo::operator==(const kuzu::common::StructTypeInfo& other) const {
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

std::unique_ptr<ExtraTypeInfo> StructTypeInfo::copy() const {
    std::vector<std::unique_ptr<StructField>> structFields{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        structFields[i] = fields[i]->copy();
    }
    return std::make_unique<StructTypeInfo>(std::move(structFields));
}

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

std::unique_ptr<LogicalType> LogicalType::copy() {
    auto dataType = std::make_unique<LogicalType>(typeID);
    if (extraTypeInfo != nullptr) {
        dataType->extraTypeInfo = extraTypeInfo->copy();
    }
    return dataType;
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
    if (dataTypeString.ends_with("[]")) {
        dataType.typeID = LogicalTypeID::VAR_LIST;
        dataType.extraTypeInfo = std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(
            dataTypeFromString(dataTypeString.substr(0, dataTypeString.size() - 2))));
    } else if (dataTypeString.ends_with("]")) {
        dataType.typeID = LogicalTypeID::FIXED_LIST;
        auto leftBracketPos = dataTypeString.find('[');
        auto rightBracketPos = dataTypeString.find(']');
        auto childType = std::make_unique<LogicalType>(
            dataTypeFromString(dataTypeString.substr(0, leftBracketPos)));
        auto fixedNumElementsInList = std::strtoll(
            dataTypeString.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1).c_str(),
            nullptr, 0 /* base */);
        dataType.extraTypeInfo =
            std::make_unique<FixedListTypeInfo>(std::move(childType), fixedNumElementsInList);
    } else if (dataTypeString.starts_with("STRUCT")) {
        dataType.typeID = LogicalTypeID::STRUCT;
        auto leftBracketPos = dataTypeString.find('(');
        auto rightBracketPos = dataTypeString.find_last_of(')');
        if (leftBracketPos == std::string::npos || rightBracketPos == std::string::npos) {
            throw Exception("Cannot parse struct type: " + dataTypeString);
        }
        // Remove the leading and trailing brackets.
        auto structTypeStr =
            dataTypeString.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
        auto structFieldsStr = parseStructFields(structTypeStr);
        std::vector<std::unique_ptr<StructField>> structFields;
        for (auto& structFieldStr : structFieldsStr) {
            auto pos = structFieldStr.find(' ');
            auto fieldName = structFieldStr.substr(0, pos);
            auto fieldTypeString = structFieldStr.substr(pos + 1);
            structFields.emplace_back(std::make_unique<StructField>(
                fieldName, std::make_unique<LogicalType>(dataTypeFromString(fieldTypeString))));
        }
        dataType.extraTypeInfo = std::make_unique<StructTypeInfo>(std::move(structFields));
    } else {
        dataType.typeID = dataTypeIDFromString(dataTypeString);
    }
    dataType.setPhysicalType();
    return dataType;
}

LogicalTypeID LogicalTypeUtils::dataTypeIDFromString(const std::string& dataTypeIDString) {
    if ("INTERNAL_ID" == dataTypeIDString) {
        return LogicalTypeID::INTERNAL_ID;
    } else if ("INT64" == dataTypeIDString) {
        return LogicalTypeID::INT64;
    } else if ("INT32" == dataTypeIDString) {
        return LogicalTypeID::INT32;
    } else if ("INT16" == dataTypeIDString) {
        return LogicalTypeID::INT16;
    } else if ("INT" == dataTypeIDString) {
        return LogicalTypeID::INT32;
    } else if ("DOUBLE" == dataTypeIDString) {
        return LogicalTypeID::DOUBLE;
    } else if ("FLOAT" == dataTypeIDString) {
        return LogicalTypeID::FLOAT;
    } else if ("BOOLEAN" == dataTypeIDString) {
        return LogicalTypeID::BOOL;
    } else if ("BYTEA" == dataTypeIDString || "BLOB" == dataTypeIDString) {
        return LogicalTypeID::BLOB;
    } else if ("STRING" == dataTypeIDString) {
        return LogicalTypeID::STRING;
    } else if ("DATE" == dataTypeIDString) {
        return LogicalTypeID::DATE;
    } else if ("TIMESTAMP" == dataTypeIDString) {
        return LogicalTypeID::TIMESTAMP;
    } else if ("INTERVAL" == dataTypeIDString) {
        return LogicalTypeID::INTERVAL;
    } else if ("SERIAL" == dataTypeIDString) {
        return LogicalTypeID::SERIAL;
    } else {
        throw NotImplementedException("Cannot parse dataTypeID: " + dataTypeIDString);
    }
}

std::string LogicalTypeUtils::dataTypeToString(const LogicalType& dataType) {
    switch (dataType.typeID) {
    case LogicalTypeID::MAP: {
        auto structType = common::VarListType::getChildType(&dataType);
        auto fieldTypes = common::StructType::getFieldTypes(structType);
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

bool LogicalTypeUtils::isNumerical(const kuzu::common::LogicalType& dataType) {
    switch (dataType.typeID) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::SERIAL:
        return true;
    default:
        return false;
    }
}

std::vector<LogicalType> LogicalTypeUtils::getAllValidComparableLogicalTypes() {
    return std::vector<LogicalType>{LogicalType{LogicalTypeID::BOOL},
        LogicalType{LogicalTypeID::INT64}, LogicalType{LogicalTypeID::INT32},
        LogicalType{LogicalTypeID::INT16}, LogicalType{LogicalTypeID::DOUBLE},
        LogicalType{LogicalTypeID::FLOAT}, LogicalType{LogicalTypeID::DATE},
        LogicalType{LogicalTypeID::TIMESTAMP}, LogicalType{LogicalTypeID::INTERVAL},
        LogicalType{LogicalTypeID::BLOB}, LogicalType{LogicalTypeID::STRING},
        LogicalType{LogicalTypeID::SERIAL}};
}

std::vector<LogicalTypeID> LogicalTypeUtils::getNumericalLogicalTypeIDs() {
    return std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::INT32,
        LogicalTypeID::INT16, LogicalTypeID::DOUBLE, LogicalTypeID::FLOAT, LogicalTypeID::SERIAL};
}

std::vector<LogicalType> LogicalTypeUtils::getAllValidLogicTypes() {
    // TODO(Ziyi): Add FIX_LIST,STRUCT,MAP type to allValidTypeID when we support functions on
    // FIXED_LIST,STRUCT,MAP.
    return std::vector<LogicalType>{LogicalType{LogicalTypeID::INTERNAL_ID},
        LogicalType{LogicalTypeID::BOOL}, LogicalType{LogicalTypeID::INT64},
        LogicalType{LogicalTypeID::INT32}, LogicalType{LogicalTypeID::INT16},
        LogicalType{LogicalTypeID::DOUBLE}, LogicalType{LogicalTypeID::STRING},
        LogicalType{LogicalTypeID::BLOB}, LogicalType{LogicalTypeID::DATE},
        LogicalType{LogicalTypeID::TIMESTAMP}, LogicalType{LogicalTypeID::INTERVAL},
        LogicalType{LogicalTypeID::VAR_LIST}, LogicalType{LogicalTypeID::FLOAT},
        LogicalType{LogicalTypeID::SERIAL}};
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

// Specialized Ser/Deser functions for logical dataTypes.
template<>
uint64_t SerDeser::serializeValue(
    const VarListTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    return SerDeser::serializeValue(*value.getChildType(), fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue(VarListTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    value.childType = std::make_unique<LogicalType>();
    offset = SerDeser::deserializeValue(*value.getChildType(), fileInfo, offset);
    return offset;
}

template<>
uint64_t SerDeser::serializeValue(
    const FixedListTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue(*value.getChildType(), fileInfo, offset);
    return SerDeser::serializeValue(value.getNumElementsInList(), fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue(FixedListTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    value.childType = std::make_unique<LogicalType>();
    offset = SerDeser::deserializeValue(*value.getChildType(), fileInfo, offset);
    offset = SerDeser::deserializeValue(value.fixedNumElementsInList, fileInfo, offset);
    return offset;
}

template<>
uint64_t SerDeser::serializeValue(
    const StructTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    return serializeVector(value.fields, fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue(StructTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    return deserializeVector(value.fields, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue(
    const std::unique_ptr<StructField>& value, FileInfo* fileInfo, uint64_t offset) {
    offset = serializeValue<std::string>(value->name, fileInfo, offset);
    return serializeValue(*value->getType(), fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue(
    std::unique_ptr<StructField>& value, FileInfo* fileInfo, uint64_t offset) {
    value = std::make_unique<StructField>();
    offset = deserializeValue<std::string>(value->name, fileInfo, offset);
    return deserializeValue(*value->type, fileInfo, offset);
}

template<>
uint64_t SerDeser::serializeValue(const LogicalType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue(value.getLogicalTypeID(), fileInfo, offset);
    switch (value.getPhysicalType()) {
    case PhysicalTypeID::VAR_LIST: {
        auto varListTypeInfo = reinterpret_cast<VarListTypeInfo*>(value.extraTypeInfo.get());
        offset = serializeValue(*varListTypeInfo, fileInfo, offset);
    } break;
    case PhysicalTypeID::FIXED_LIST: {
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(value.extraTypeInfo.get());
        offset = serializeValue(*fixedListTypeInfo, fileInfo, offset);
    } break;
    case PhysicalTypeID::STRUCT: {
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(value.extraTypeInfo.get());
        offset = serializeValue(*structTypeInfo, fileInfo, offset);
    } break;
    default:
        break;
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeValue(LogicalType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue(value.typeID, fileInfo, offset);
    value.setPhysicalType();
    switch (value.getPhysicalType()) {
    case PhysicalTypeID::VAR_LIST: {
        value.extraTypeInfo = std::make_unique<VarListTypeInfo>();
        offset = deserializeValue(
            *reinterpret_cast<VarListTypeInfo*>(value.extraTypeInfo.get()), fileInfo, offset);

    } break;
    case PhysicalTypeID::FIXED_LIST: {
        value.extraTypeInfo = std::make_unique<FixedListTypeInfo>();
        offset = deserializeValue(
            *reinterpret_cast<FixedListTypeInfo*>(value.extraTypeInfo.get()), fileInfo, offset);
    } break;
    case PhysicalTypeID::STRUCT: {
        value.extraTypeInfo = std::make_unique<StructTypeInfo>();
        offset = deserializeValue(
            *reinterpret_cast<StructTypeInfo*>(value.extraTypeInfo.get()), fileInfo, offset);
    } break;
    default:
        break;
    }
    return offset;
}

} // namespace common
} // namespace kuzu
