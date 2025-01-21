#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "common/enums/conflict_action.h"
#include "parsed_property_definition.h"

namespace kuzu {
namespace parser {

struct ExtraCreateTableInfo {
    virtual ~ExtraCreateTableInfo() = default;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
};

struct CreateTableInfo {
    catalog::CatalogEntryType type;
    std::string tableName;
    std::vector<ParsedPropertyDefinition> propertyDefinitions;
    std::unique_ptr<ExtraCreateTableInfo> extraInfo;
    common::ConflictAction onConflict;

    CreateTableInfo(catalog::CatalogEntryType type, std::string tableName,
        common::ConflictAction onConflict)
        : type{type}, tableName{std::move(tableName)}, extraInfo{nullptr}, onConflict{onConflict} {}
    DELETE_COPY_DEFAULT_MOVE(CreateTableInfo);
};

struct ExtraCreateNodeTableInfo : public ExtraCreateTableInfo {
    std::string pKName;

    explicit ExtraCreateNodeTableInfo(std::string pKName) : pKName{std::move(pKName)} {}
};

struct ExtraCreateRelTableInfo : public ExtraCreateTableInfo {
    std::string relMultiplicity;
    std::string srcTableName;
    std::string dstTableName;
    options_t options;

    ExtraCreateRelTableInfo(std::string relMultiplicity, std::string srcTableName,
        std::string dstTableName, options_t options)
        : relMultiplicity{std::move(relMultiplicity)}, srcTableName{std::move(srcTableName)},
          dstTableName{std::move(dstTableName)}, options{std::move(options)} {}
};

struct ExtraCreateRelTableGroupInfo : public ExtraCreateTableInfo {
    std::string relMultiplicity;
    std::vector<std::pair<std::string, std::string>> srcDstTablePairs;
    options_t options;

    ExtraCreateRelTableGroupInfo(std::string relMultiplicity,
        std::vector<std::pair<std::string, std::string>> srcDstTablePairs, options_t options)
        : relMultiplicity{std::move(relMultiplicity)},
          srcDstTablePairs{std::move(srcDstTablePairs)}, options{std::move(options)} {}
};

} // namespace parser
} // namespace kuzu
