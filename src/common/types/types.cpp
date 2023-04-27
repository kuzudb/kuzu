#include "common/types/types.h"

#include <sstream>
#include <stdexcept>

#include "common/exception.h"
#include "common/ser_deser.h"
#include "common/types/types_include.h"

namespace kuzu {
namespace common {

template<>
uint64_t SerDeser::serializeValue(
    const VarListTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    return SerDeser::serializeValue(*value.getChildType(), fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue(VarListTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    value.childType = std::make_unique<DataType>();
    offset = SerDeser::deserializeValue(*value.getChildType(), fileInfo, offset);
    return offset;
}

template<>
uint64_t SerDeser::serializeValue(
    const FixedListTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue(*value.getChildType(), fileInfo, offset);
    return SerDeser::serializeValue(value.getFixedNumElementsInList(), fileInfo, offset);
}

template<>
uint64_t SerDeser::deserializeValue(FixedListTypeInfo& value, FileInfo* fileInfo, uint64_t offset) {
    value.childType = std::make_unique<DataType>();
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
uint64_t SerDeser::serializeValue(const DataType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::serializeValue(value.typeID, fileInfo, offset);
    switch (value.getTypeID()) {
    case VAR_LIST: {
        auto varListTypeInfo = reinterpret_cast<VarListTypeInfo*>(value.getExtraTypeInfo());
        offset = serializeValue(*varListTypeInfo, fileInfo, offset);
    } break;
    case FIXED_LIST: {
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(value.getExtraTypeInfo());
        offset = serializeValue(*fixedListTypeInfo, fileInfo, offset);
    } break;
    case STRUCT: {
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(value.getExtraTypeInfo());
        offset = serializeValue(*structTypeInfo, fileInfo, offset);
    } break;
    default:
        break;
    }
    return offset;
}

template<>
uint64_t SerDeser::deserializeValue(DataType& value, FileInfo* fileInfo, uint64_t offset) {
    offset = SerDeser::deserializeValue(value.typeID, fileInfo, offset);
    switch (value.getTypeID()) {
    case VAR_LIST: {
        value.extraTypeInfo = std::make_unique<VarListTypeInfo>();
        offset = deserializeValue(
            *reinterpret_cast<VarListTypeInfo*>(value.getExtraTypeInfo()), fileInfo, offset);

    } break;
    case FIXED_LIST: {
        value.extraTypeInfo = std::make_unique<FixedListTypeInfo>();
        offset = deserializeValue(
            *reinterpret_cast<FixedListTypeInfo*>(value.getExtraTypeInfo()), fileInfo, offset);
    } break;
    case STRUCT: {
        value.extraTypeInfo = std::make_unique<StructTypeInfo>();
        offset = deserializeValue(
            *reinterpret_cast<StructTypeInfo*>(value.getExtraTypeInfo()), fileInfo, offset);
    } break;
    default:
        break;
    }
    return offset;
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

bool StructField::operator==(const kuzu::common::StructField& other) const {
    return name == other.name && *type == *other.type;
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

std::unique_ptr<StructField> StructField::copy() const {
    return std::make_unique<StructField>(name, type->copy());
}

std::unique_ptr<ExtraTypeInfo> StructTypeInfo::copy() const {
    std::vector<std::unique_ptr<StructField>> structFields{fields.size()};
    for (auto i = 0u; i < fields.size(); i++) {
        structFields[i] = fields[i]->copy();
    }
    return std::make_unique<StructTypeInfo>(std::move(structFields));
}

std::vector<DataType*> StructTypeInfo::getChildrenTypes() const {
    std::vector<DataType*> childrenTypesToReturn{fields.size()};
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

DataType::DataType(const DataType& other) {
    typeID = other.typeID;
    if (other.extraTypeInfo != nullptr) {
        extraTypeInfo = other.getExtraTypeInfo()->copy();
    }
}

DataType::DataType(DataType&& other) noexcept
    : typeID{other.typeID}, extraTypeInfo{std::move(other.extraTypeInfo)} {}

std::vector<DataTypeID> DataType::getNumericalTypeIDs() {
    return std::vector<DataTypeID>{INT64, INT32, INT16, DOUBLE, FLOAT};
}

std::vector<DataTypeID> DataType::getAllValidComparableTypes() {
    return std::vector<DataTypeID>{
        BOOL, INT64, INT32, INT16, DOUBLE, FLOAT, DATE, TIMESTAMP, INTERVAL, STRING};
}

std::vector<DataTypeID> DataType::getAllValidTypeIDs() {
    // TODO(Ziyi): Add FIX_LIST type to allValidTypeID when we support functions on VAR_LIST.
    return std::vector<DataTypeID>{INTERNAL_ID, BOOL, INT64, INT32, INT16, DOUBLE, STRING, DATE,
        TIMESTAMP, INTERVAL, VAR_LIST, FLOAT};
}

DataType& DataType::operator=(const DataType& other) {
    typeID = other.typeID;
    if (other.extraTypeInfo != nullptr) {
        extraTypeInfo = other.extraTypeInfo->copy();
    }
    return *this;
}

bool DataType::operator==(const DataType& other) const {
    if (typeID != other.typeID) {
        return false;
    }
    switch (other.typeID) {
    case VAR_LIST:
        return *reinterpret_cast<VarListTypeInfo*>(extraTypeInfo.get()) ==
               *reinterpret_cast<VarListTypeInfo*>(other.extraTypeInfo.get());
    case FIXED_LIST:
        return *reinterpret_cast<FixedListTypeInfo*>(extraTypeInfo.get()) ==
               *reinterpret_cast<FixedListTypeInfo*>(other.extraTypeInfo.get());
    case STRUCT:
        return *reinterpret_cast<StructTypeInfo*>(extraTypeInfo.get()) ==
               *reinterpret_cast<StructTypeInfo*>(other.extraTypeInfo.get());
    default:
        return true;
    }
}

bool DataType::operator!=(const DataType& other) const {
    return !((*this) == other);
}

DataType& DataType::operator=(DataType&& other) noexcept {
    typeID = other.typeID;
    extraTypeInfo = std::move(other.extraTypeInfo);
    return *this;
}

std::unique_ptr<DataType> DataType::copy() {
    auto dataType = std::make_unique<DataType>(typeID);
    if (extraTypeInfo != nullptr) {
        dataType->extraTypeInfo = extraTypeInfo->copy();
    }
    return dataType;
}

ExtraTypeInfo* DataType::getExtraTypeInfo() const {
    return extraTypeInfo.get();
}

DataTypeID DataType::getTypeID() const {
    return typeID;
}

DataType* DataType::getChildType() const {
    assert(typeID == FIXED_LIST || typeID == VAR_LIST);
    return reinterpret_cast<VarListTypeInfo*>(extraTypeInfo.get())->getChildType();
}

DataType Types::dataTypeFromString(const std::string& dataTypeString) {
    DataType dataType;
    if (dataTypeString.ends_with("[]")) {
        dataType.typeID = VAR_LIST;
        dataType.extraTypeInfo = std::make_unique<VarListTypeInfo>(std::make_unique<DataType>(
            dataTypeFromString(dataTypeString.substr(0, dataTypeString.size() - 2))));
    } else if (dataTypeString.ends_with("]")) {
        dataType.typeID = FIXED_LIST;
        auto leftBracketPos = dataTypeString.find('[');
        auto rightBracketPos = dataTypeString.find(']');
        auto childType = std::make_unique<DataType>(
            dataTypeFromString(dataTypeString.substr(0, leftBracketPos)));
        auto fixedNumElementsInList = std::strtoll(
            dataTypeString.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1).c_str(),
            nullptr, 0 /* base */);
        dataType.extraTypeInfo =
            std::make_unique<FixedListTypeInfo>(std::move(childType), fixedNumElementsInList);
    } else if (dataTypeString.starts_with("STRUCT")) {
        dataType.typeID = STRUCT;
        auto leftBracketPos = dataTypeString.find('(');
        auto rightBracketPos = dataTypeString.find(')');
        if (leftBracketPos == std::string::npos || rightBracketPos == std::string::npos) {
            throw Exception("Cannot parse struct type: " + dataTypeString);
        }
        std::istringstream iss{
            dataTypeString.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1)};
        std::vector<std::unique_ptr<StructField>> childrenTypes;
        std::string field, fieldName, fieldType;
        while (getline(iss, field, ',')) {
            std::istringstream fieldStream{field};
            // The first word is the field name.
            fieldStream >> fieldName;
            // The second word is the field type.
            fieldStream >> fieldType;
            childrenTypes.emplace_back(std::make_unique<StructField>(
                fieldName, std::make_unique<DataType>(dataTypeFromString(fieldType))));
        }
        dataType.extraTypeInfo = std::make_unique<StructTypeInfo>(std::move(childrenTypes));
    } else {
        dataType.typeID = dataTypeIDFromString(dataTypeString);
    }
    return dataType;
}

DataTypeID Types::dataTypeIDFromString(const std::string& dataTypeIDString) {
    if ("INTERNAL_ID" == dataTypeIDString) {
        return INTERNAL_ID;
    } else if ("INT64" == dataTypeIDString) {
        return INT64;
    } else if ("INT32" == dataTypeIDString) {
        return INT32;
    } else if ("INT16" == dataTypeIDString) {
        return INT16;
    } else if ("INT" == dataTypeIDString) {
        return INT32;
    } else if ("DOUBLE" == dataTypeIDString) {
        return DOUBLE;
    } else if ("FLOAT" == dataTypeIDString) {
        return FLOAT;
    } else if ("BOOLEAN" == dataTypeIDString) {
        return BOOL;
    } else if ("STRING" == dataTypeIDString) {
        return STRING;
    } else if ("DATE" == dataTypeIDString) {
        return DATE;
    } else if ("TIMESTAMP" == dataTypeIDString) {
        return TIMESTAMP;
    } else if ("INTERVAL" == dataTypeIDString) {
        return INTERVAL;
    } else if ("SERIAL" == dataTypeIDString) {
        return SERIAL;
    } else {
        throw InternalException("Cannot parse dataTypeID: " + dataTypeIDString);
    }
}

std::string Types::dataTypeToString(const DataType& dataType) {
    switch (dataType.typeID) {
    case VAR_LIST: {
        auto varListTypeInfo = reinterpret_cast<VarListTypeInfo*>(dataType.extraTypeInfo.get());
        return dataTypeToString(*varListTypeInfo->getChildType()) + "[]";
    }
    case FIXED_LIST: {
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(dataType.extraTypeInfo.get());
        return dataTypeToString(*fixedListTypeInfo->getChildType()) + "[" +
               std::to_string(fixedListTypeInfo->getFixedNumElementsInList()) + "]";
    }
    case STRUCT: {
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(dataType.extraTypeInfo.get());
        std::string dataTypeStr = dataTypeToString(dataType.typeID) + "(";
        auto numFields = structTypeInfo->getChildrenTypes().size();
        for (auto i = 0u; i < numFields - 1; i++) {
            dataTypeStr += dataTypeToString(*structTypeInfo->getChildrenTypes()[i]);
            dataTypeStr += ",";
        }
        dataTypeStr += dataTypeToString(*structTypeInfo->getChildrenTypes()[numFields - 1]);
        return dataTypeStr + ")";
    }
    case ANY:
    case NODE:
    case REL:
    case INTERNAL_ID:
    case BOOL:
    case INT64:
    case INT32:
    case INT16:
    case DOUBLE:
    case FLOAT:
    case DATE:
    case TIMESTAMP:
    case INTERVAL:
    case STRING:
        return dataTypeToString(dataType.typeID);
    default:
        throw InternalException("Unsupported DataType: " + Types::dataTypeToString(dataType) + ".");
    }
}

std::string Types::dataTypeToString(DataTypeID dataTypeID) {
    switch (dataTypeID) {
    case ANY:
        return "ANY";
    case NODE:
        return "NODE";
    case REL:
        return "REL";
    case INTERNAL_ID:
        return "INTERNAL_ID";
    case BOOL:
        return "BOOL";
    case INT64:
        return "INT64";
    case INT32:
        return "INT32";
    case INT16:
        return "INT16";
    case DOUBLE:
        return "DOUBLE";
    case FLOAT:
        return "FLOAT";
    case DATE:
        return "DATE";
    case TIMESTAMP:
        return "TIMESTAMP";
    case INTERVAL:
        return "INTERVAL";
    case STRING:
        return "STRING";
    case VAR_LIST:
        return "VAR_LIST";
    case FIXED_LIST:
        return "FIXED_LIST";
    case STRUCT:
        return "STRUCT";
    case SERIAL:
        return "SERIAL";
    default:
        throw InternalException(
            "Unsupported DataType: " + Types::dataTypeToString(dataTypeID) + ".");
    }
}

std::string Types::dataTypesToString(const std::vector<DataType>& dataTypes) {
    std::vector<DataTypeID> dataTypeIDs;
    for (auto& dataType : dataTypes) {
        dataTypeIDs.push_back(dataType.typeID);
    }
    return dataTypesToString(dataTypeIDs);
}

std::string Types::dataTypesToString(const std::vector<DataTypeID>& dataTypeIDs) {
    if (dataTypeIDs.empty()) {
        return {""};
    }
    std::string result = "(" + Types::dataTypeToString(dataTypeIDs[0]);
    for (auto i = 1u; i < dataTypeIDs.size(); ++i) {
        result += "," + Types::dataTypeToString(dataTypeIDs[i]);
    }
    result += ")";
    return result;
}

uint32_t Types::getDataTypeSize(DataTypeID dataTypeID) {
    switch (dataTypeID) {
    case INTERNAL_ID:
        return sizeof(internalID_t);
    case BOOL:
        return sizeof(uint8_t);
    case SERIAL:
    case INT64:
        return sizeof(int64_t);
    case INT32:
        return sizeof(int32_t);
    case INT16:
        return sizeof(int16_t);
    case DOUBLE:
        return sizeof(double_t);
    case FLOAT:
        return sizeof(float_t);
    case DATE:
        return sizeof(date_t);
    case TIMESTAMP:
        return sizeof(timestamp_t);
    case INTERVAL:
        return sizeof(interval_t);
    case STRING:
        return sizeof(ku_string_t);
    case VAR_LIST:
        return sizeof(ku_list_t);
    default:
        throw InternalException(
            "Cannot infer the size of dataTypeID: " + dataTypeToString(dataTypeID) + ".");
    }
}

uint32_t Types::getDataTypeSize(const DataType& dataType) {
    switch (dataType.typeID) {
    case FIXED_LIST: {
        auto fixedListTypeInfo = reinterpret_cast<FixedListTypeInfo*>(dataType.extraTypeInfo.get());
        return getDataTypeSize(*fixedListTypeInfo->getChildType()) *
               fixedListTypeInfo->getFixedNumElementsInList();
    }
    case STRUCT: {
        auto structTypeInfo = reinterpret_cast<StructTypeInfo*>(dataType.extraTypeInfo.get());
        uint32_t size = 0;
        for (auto& childType : structTypeInfo->getChildrenTypes()) {
            size += getDataTypeSize(*childType);
        }
        return size;
    }
    case INTERNAL_ID:
    case BOOL:
    case SERIAL:
    case INT64:
    case INT32:
    case INT16:
    case DOUBLE:
    case FLOAT:
    case DATE:
    case TIMESTAMP:
    case INTERVAL:
    case STRING:
    case VAR_LIST: {
        return getDataTypeSize(dataType.typeID);
    }
    default: {
        throw InternalException(
            "Cannot infer the size of dataTypeID: " + dataTypeToString(dataType.typeID) + ".");
    }
    }
}

bool Types::isNumerical(const kuzu::common::DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
    case INT32:
    case INT16:
    case DOUBLE:
    case FLOAT:
        return true;
    default:
        return false;
    }
}

RelDirection operator!(RelDirection& direction) {
    return (FWD == direction) ? BWD : FWD;
}

std::string getRelDirectionAsString(RelDirection direction) {
    return (FWD == direction) ? "forward" : "backward";
}

} // namespace common
} // namespace kuzu
