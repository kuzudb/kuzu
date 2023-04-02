#include "common/types/types.h"

#include <stdexcept>

#include "common/exception.h"
#include "common/types/types_include.h"

namespace kuzu {
namespace common {

DataType::DataType() : typeID{ANY}, childType{nullptr} {}

DataType::DataType(DataTypeID typeID) : typeID{typeID}, childType{nullptr} {
    assert(typeID != VAR_LIST && typeID != FIXED_LIST);
}

DataType::DataType(DataTypeID typeID, std::unique_ptr<DataType> childType)
    : typeID{typeID}, childType{std::move(childType)} {
    assert(typeID == VAR_LIST);
}

DataType::DataType(
    DataTypeID typeID, std::unique_ptr<DataType> childType, uint64_t fixedNumElementsInList)
    : typeID{typeID}, childType{std::move(childType)}, fixedNumElementsInList{
                                                           fixedNumElementsInList} {
    assert(typeID == FIXED_LIST);
}

DataType::DataType(const DataType& other) {
    switch (other.typeID) {
    case FIXED_LIST:
    case VAR_LIST: {
        typeID = other.typeID;
        childType = other.childType->copy();
        fixedNumElementsInList = other.fixedNumElementsInList;
    } break;
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
    case STRING: {
        typeID = other.typeID;
    } break;
    default:
        throw InternalException("Unsupported DataType: " + Types::dataTypeToString(other) + ".");
    }
}

DataType::DataType(DataType&& other) noexcept
    : typeID{other.typeID}, childType{std::move(other.childType)},
      fixedNumElementsInList{other.fixedNumElementsInList} {}

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
    switch (other.typeID) {
    case FIXED_LIST:
    case VAR_LIST: {
        typeID = other.typeID;
        childType = other.childType->copy();
        fixedNumElementsInList = other.fixedNumElementsInList;
    } break;
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
    case STRING: {
        typeID = other.typeID;
    } break;
    default:
        throw InternalException("Unsupported DataType: " + Types::dataTypeToString(other) + ".");
    }
    return *this;
}

bool DataType::operator==(const DataType& other) const {
    switch (other.typeID) {
    case FIXED_LIST: {
        return typeID == other.typeID && *childType == *other.childType &&
               fixedNumElementsInList == other.fixedNumElementsInList;
    }
    case VAR_LIST: {
        return typeID == other.typeID && *childType == *other.childType;
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
        return typeID == other.typeID;
    default:
        throw InternalException("Unsupported DataType: " + Types::dataTypeToString(other) + ".");
    }
}

bool DataType::operator!=(const DataType& other) const {
    return !((*this) == other);
}

DataType& DataType::operator=(DataType&& other) noexcept {
    typeID = other.typeID;
    childType = std::move(other.childType);
    fixedNumElementsInList = other.fixedNumElementsInList;
    return *this;
}

std::unique_ptr<DataType> DataType::copy() {
    switch (typeID) {
    case FIXED_LIST: {
        return make_unique<DataType>(typeID, childType->copy(), fixedNumElementsInList);
    }
    case VAR_LIST: {
        return make_unique<DataType>(typeID, childType->copy());
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
        return std::make_unique<DataType>(typeID);
    default:
        throw InternalException("Unsupported DataType: " + Types::dataTypeToString(typeID) + ".");
    }
}

DataTypeID DataType::getTypeID() const {
    return typeID;
}

DataType* DataType::getChildType() const {
    return childType.get();
}

DataType Types::dataTypeFromString(const std::string& dataTypeString) {
    DataType dataType;
    if (dataTypeString.ends_with("[]")) {
        dataType.typeID = VAR_LIST;
        dataType.childType = std::make_unique<DataType>(
            dataTypeFromString(dataTypeString.substr(0, dataTypeString.size() - 2)));
    } else if (dataTypeString.ends_with("]")) {
        dataType.typeID = FIXED_LIST;
        auto leftBracketPos = dataTypeString.find('[');
        auto rightBracketPos = dataTypeString.find(']');
        dataType.childType = std::make_unique<DataType>(
            dataTypeFromString(dataTypeString.substr(0, leftBracketPos)));
        dataType.fixedNumElementsInList = std::strtoll(
            dataTypeString.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1).c_str(),
            nullptr, 0 /* base */);
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
    } else {
        throw InternalException("Cannot parse dataTypeID: " + dataTypeIDString);
    }
}

std::string Types::dataTypeToString(const DataType& dataType) {
    switch (dataType.typeID) {
    case VAR_LIST: {
        assert(dataType.childType);
        return dataTypeToString(*dataType.childType) + "[]";
    }
    case FIXED_LIST: {
        assert(dataType.childType);
        return dataTypeToString(*dataType.childType) + "[" +
               std::to_string(dataType.fixedNumElementsInList) + "]";
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
    case FIXED_LIST:
        return getDataTypeSize(*dataType.childType) * dataType.fixedNumElementsInList;
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
    case VAR_LIST:
        return getDataTypeSize(dataType.typeID);
    default:
        throw InternalException(
            "Cannot infer the size of dataTypeID: " + dataTypeToString(dataType.typeID) + ".");
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
