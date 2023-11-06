#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/enums/table_type.h"

namespace kuzu {
namespace parser {

struct ExtraCreateTableInfo {
    virtual ~ExtraCreateTableInfo() = default;
};

struct CreateTableInfo {
    common::TableType tableType;
    std::string tableName;
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes;
    std::unique_ptr<ExtraCreateTableInfo> extraInfo;

    CreateTableInfo(common::TableType tableType, std::string tableName)
        : tableType{tableType}, tableName{std::move(tableName)}, extraInfo{nullptr} {}
    CreateTableInfo(common::TableType tableType, std::string tableName,
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes,
        std::unique_ptr<ExtraCreateTableInfo> extraInfo)
        : tableType{tableType}, tableName{std::move(tableName)},
          propertyNameDataTypes{std::move(propertyNameDataTypes)}, extraInfo{
                                                                       std::move(extraInfo)} {};
};

struct ExtraCreateNodeTableInfo : public ExtraCreateTableInfo {
    std::string pKName;

    explicit ExtraCreateNodeTableInfo(std::string pKName) : pKName{std::move(pKName)} {}
};

struct ExtraCreateRelTableInfo : public ExtraCreateTableInfo {
    std::string relMultiplicity;
    std::string srcTableName;
    std::string dstTableName;

    ExtraCreateRelTableInfo(
        std::string relMultiplicity, std::string srcTableName, std::string dstTableName)
        : relMultiplicity{std::move(relMultiplicity)}, srcTableName{std::move(srcTableName)},
          dstTableName{std::move(dstTableName)} {}
};

struct ExtraCreateRelTableGroupInfo : public ExtraCreateTableInfo {
    std::string relMultiplicity;
    std::vector<std::pair<std::string, std::string>> srcDstTablePairs;

    ExtraCreateRelTableGroupInfo(std::string relMultiplicity,
        std::vector<std::pair<std::string, std::string>> srcDstTablePairs)
        : relMultiplicity{std::move(relMultiplicity)}, srcDstTablePairs{
                                                           std::move(srcDstTablePairs)} {}
};

} // namespace parser
} // namespace kuzu
