#include "connector/duckdb_type_converter.h"

#include "common/exception/binder.h"
#include "common/string_utils.h"

namespace kuzu {
namespace duckdb_extension {

using namespace kuzu::common;

common::LogicalType DuckDBTypeConverter::convertDuckDBType(std::string typeStr) {
    typeStr = common::StringUtils::ltrim(typeStr);
    typeStr = common::StringUtils::rtrim(typeStr);
    if (typeStr == "BIGINT" || typeStr == "INT8" || typeStr == "LONG") {
        return LogicalType{LogicalTypeID::INT64};
    } else if (typeStr == "BLOB" || typeStr == "BYTEA" || typeStr == "BINARY" ||
               typeStr == "VARBINARY") {
        return LogicalType{LogicalTypeID::BLOB};
    } else if (typeStr == "BOOLEAN" || typeStr == "BOOL" || typeStr == "LOGICAL") {
        return LogicalType{LogicalTypeID::BOOL};
    } else if (typeStr == "DATE") {
        return LogicalType{LogicalTypeID::DATE};
    } else if (typeStr == "DOUBLE" || typeStr == "FLOAT8") {
        return LogicalType{LogicalTypeID::DOUBLE};
    } else if (typeStr == "HUGEINT") {
        return LogicalType{LogicalTypeID::INT128};
    } else if (typeStr == "INTEGER" || typeStr == "INT4" || typeStr == "INT" ||
               typeStr == "SIGNED") {
        return LogicalType{LogicalTypeID::INT32};
    } else if (typeStr == "INTERVAL") {
        return LogicalType{LogicalTypeID::INTERVAL};
    } else if (typeStr == "REAL" || typeStr == "FLOAT" || typeStr == "FLOAT4") {
        return LogicalType{LogicalTypeID::FLOAT};
    } else if (typeStr == "SMALLINT" || typeStr == "INT2" || typeStr == "SHORT") {
        return LogicalType{LogicalTypeID::INT16};
    } else if (typeStr == "TIMESTAMP" || typeStr == "DATETIME") {
        return LogicalType{LogicalTypeID::TIMESTAMP};
    } else if (typeStr == "TIMESTAMP_NS") {
        return LogicalType{LogicalTypeID::TIMESTAMP_NS};
    } else if (typeStr == "TIMESTAMP_MS") {
        return LogicalType{LogicalTypeID::TIMESTAMP_MS};
    } else if (typeStr == "TIMESTAMP_S") {
        return LogicalType{LogicalTypeID::TIMESTAMP_SEC};
    } else if (typeStr == "TIMESTAMP WITH TIME ZONE" || typeStr == "TIMESTAMPTZ") {
        return LogicalType{LogicalTypeID::TIMESTAMP_TZ};
    } else if (typeStr == "TINYINT" || typeStr == "INT1") {
        return LogicalType{LogicalTypeID::INT8};
    } else if (typeStr == "UBIGINT") {
        return LogicalType{LogicalTypeID::UINT64};
    } else if (typeStr == "UINTEGER") {
        return LogicalType{LogicalTypeID::UINT32};
    } else if (typeStr == "USMALLINT") {
        return LogicalType{LogicalTypeID::UINT16};
    } else if (typeStr == "UTINYINT") {
        return LogicalType{LogicalTypeID::UINT8};
    } else if (typeStr == "UUID") {
        return LogicalType{LogicalTypeID::UUID};
    } else if (typeStr == "VARCHAR" || typeStr == "CHAR" || typeStr == "BPCHAR" ||
               typeStr == "TEXT" || typeStr == "STRING") {
        return LogicalType{LogicalTypeID::STRING};
    } else if (typeStr.ends_with("[]")) {
        auto innerType = convertDuckDBType(typeStr.substr(0, typeStr.size() - 2));
        return LogicalType::LIST(innerType.copy());
    } else if (typeStr.starts_with("STRUCT")) {
        return LogicalType::STRUCT(parseStructTypeInfo(typeStr));
    } else if (typeStr.starts_with("UNION")) {
        auto unionFields = parseStructTypeInfo(typeStr);
        return LogicalType::UNION(std::move(unionFields));
    } else if (typeStr.starts_with("MAP")) {
        auto leftBracketPos = typeStr.find('(');
        auto rightBracketPos = typeStr.find_last_of(')');
        auto mapTypeStr = typeStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
        auto keyValueTypes = StringUtils::splitComma(mapTypeStr);
        return LogicalType::MAP(convertDuckDBType(keyValueTypes[0]),
            convertDuckDBType(keyValueTypes[1]));
    } else if (typeStr.ends_with(']')) {
        auto leftBracketPos = typeStr.find('[');
        auto rightBracketPos = typeStr.find(']');
        auto numValuesInList = std::strtoll(
            typeStr.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1).c_str(),
            nullptr, 0);
        auto innerType = convertDuckDBType(typeStr.substr(0, leftBracketPos));
        return LogicalType::ARRAY(innerType.copy(), numValuesInList);
    } else if (typeStr.starts_with("DECIMAL")) {
        auto decimalInfoLeftBracket = typeStr.find('(');
        auto decimalInfoSeparator = typeStr.find(',');
        auto decimalInfoRightBracket = typeStr.find(')');
        auto width = std::strtoll(typeStr
                                      .substr(decimalInfoLeftBracket + 1,
                                          decimalInfoSeparator - decimalInfoLeftBracket - 1)
                                      .c_str(),
            nullptr /* endPtr */, 0);
        auto scale = std::strtoll(typeStr
                                      .substr(decimalInfoSeparator + 1,
                                          decimalInfoRightBracket - decimalInfoSeparator - 1)
                                      .c_str(),
            nullptr /* endPtr */, 0);
        return LogicalType::DECIMAL(width, scale);
    }
    throw BinderException{stringFormat("Unsupported duckdb type: {}.", typeStr)};
}

std::vector<std::string> DuckDBTypeConverter::parseStructFields(const std::string& structTypeStr) {
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

std::vector<StructField> DuckDBTypeConverter::parseStructTypeInfo(
    const std::string& structTypeStr) {
    auto leftBracketPos = structTypeStr.find('(');
    auto rightBracketPos = structTypeStr.find_last_of(')');
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
            LogicalType(DuckDBTypeConverter::convertDuckDBType(fieldTypeString)));
    }
    return structFields;
}

} // namespace duckdb_extension
} // namespace kuzu
