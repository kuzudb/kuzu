#include "include/types.h"

#include <stdexcept>

#include "include/types_include.h"
#include "include/value.h"

namespace graphflow {
namespace common {

DataType::DataType(const DataType& other) : typeID{other.typeID} {
    if (other.childType) {
        childType = other.childType->copy();
    }
}

DataType& DataType::operator=(const DataType& other) {
    typeID = other.typeID;
    if (childType) {
        childType = other.childType->copy();
    }
    return *this;
}

unique_ptr<DataType> DataType::copy() {
    if (childType) {
        return make_unique<DataType>(typeID, childType->copy());
    } else {
        return make_unique<DataType>(typeID);
    }
}

DataType Types::dataTypeFromString(const string& dataTypeString) {
    DataType dataType;
    if (dataTypeString.ends_with("[]")) {
        dataType.typeID = LIST;
        dataType.childType = make_unique<DataType>(
            dataTypeFromString(dataTypeString.substr(0, dataTypeString.size() - 2)));
        return dataType;
    } else {
        dataType.typeID = dataTypeIDFromString(dataTypeString);
    }
    return dataType;
}

DataTypeID Types::dataTypeIDFromString(const std::string& dataTypeIDString) {
    if ("LABEL" == dataTypeIDString) {
        return LABEL;
    } else if ("NODE" == dataTypeIDString) {
        return NODE;
    } else if ("REL" == dataTypeIDString) {
        return REL;
    } else if ("INT64" == dataTypeIDString) {
        return INT64;
    } else if ("DOUBLE" == dataTypeIDString) {
        return DOUBLE;
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
        throw invalid_argument("Cannot parse dataTypeID: " + dataTypeIDString);
    }
}

string Types::dataTypeToString(const DataType& dataType) {
    if (dataType.typeID == LIST) {
        assert(dataType.childType);
        auto result = dataTypeToString(*dataType.childType) + "[]";
        return result;
    } else {
        return dataTypeToString(dataType.typeID);
    }
}

string Types::dataTypeToString(DataTypeID dataTypeID) {
    assert(dataTypeID != LIST);
    return DataTypeIdNames[dataTypeID];
}

string Types::dataTypesToString(const vector<DataType>& dataTypes) {
    vector<DataTypeID> dataTypeIDs;
    for (auto& dataType : dataTypes) {
        dataTypeIDs.push_back(dataType.typeID);
    }
    return dataTypesToString(dataTypeIDs);
}

string Types::dataTypesToString(const vector<DataTypeID>& dataTypeIDs) {
    if (dataTypeIDs.empty()) {
        return string("");
    }
    string result = "(" + Types::dataTypeToString(dataTypeIDs[0]);
    for (auto i = 1u; i < dataTypeIDs.size(); ++i) {
        result += "," + Types::dataTypeToString(dataTypeIDs[i]);
    }
    result += ")";
    return result;
}

bool Types::isNumericalType(DataTypeID dataTypeID) {
    return dataTypeID == INT64 || dataTypeID == DOUBLE;
}

bool Types::isLiteralType(DataTypeID dataTypeID) {
    return dataTypeID == BOOL || dataTypeID == INT64 || dataTypeID == DOUBLE ||
           dataTypeID == STRING || dataTypeID == DATE || dataTypeID == TIMESTAMP ||
           dataTypeID == INTERVAL;
}

size_t Types::getDataTypeSize(DataTypeID dataTypeID) {
    switch (dataTypeID) {
    case LABEL:
        return sizeof(label_t);
    case NODE:
        return sizeof(nodeID_t);
    case INT64:
        return sizeof(int64_t);
    case DOUBLE:
        return sizeof(double_t);
    case BOOL:
        return sizeof(uint8_t);
    case STRING:
        return sizeof(gf_string_t);
    case UNSTRUCTURED:
        return sizeof(Value);
    case DATE:
        return sizeof(date_t);
    case TIMESTAMP:
        return sizeof(timestamp_t);
    case INTERVAL:
        return sizeof(interval_t);
    case LIST:
        return sizeof(gf_list_t);
    default:
        throw invalid_argument(
            "Cannot infer the size of dataTypeID: " + dataTypeToString(dataTypeID) + ".");
    }
}

size_t Types::getDataTypeSize(const DataType& dataType) {
    return getDataTypeSize(dataType.typeID);
}

RelDirection operator!(RelDirection& direction) {
    return (FWD == direction) ? BWD : FWD;
}

} // namespace common
} // namespace graphflow
