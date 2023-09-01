#pragma once

#include <string>
#include <utility>
#include <vector>

namespace kuzu {
namespace parser {

struct ExtraCreateTableInfo {
    virtual ~ExtraCreateTableInfo() = default;
};

struct CreateTableInfo {
    std::string tableName;
    std::vector<std::pair<std::string, std::string>> propertyNameDataTypes;
    std::unique_ptr<ExtraCreateTableInfo> extraInfo;

    explicit CreateTableInfo(std::string tableName)
        : tableName{std::move(tableName)}, extraInfo{nullptr} {}
    CreateTableInfo(std::string tableName,
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes,
        std::unique_ptr<ExtraCreateTableInfo> extraInfo)
        : tableName{std::move(tableName)}, propertyNameDataTypes{std::move(propertyNameDataTypes)},
          extraInfo{std::move(extraInfo)} {};
};

struct NodeExtraCreateTableInfo : public ExtraCreateTableInfo {
    std::string pKName;

    explicit NodeExtraCreateTableInfo(std::string pKName) : pKName{std::move(pKName)} {}
};

struct RelExtraCreateTableInfo : public ExtraCreateTableInfo {
    std::string relMultiplicity;
    std::string srcTableName;
    std::string dstTableName;

    RelExtraCreateTableInfo(
        std::string relMultiplicity, std::string srcTableName, std::string dstTableName)
        : relMultiplicity{std::move(relMultiplicity)}, srcTableName{std::move(srcTableName)},
          dstTableName{std::move(dstTableName)} {}
};

} // namespace parser
} // namespace kuzu
