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
    options_t options;
    std::string srcTableName;
    std::string dstTableName;

    ExtraCreateRelTableInfo(std::string relMultiplicity, options_t options,
        std::string srcTableName, std::string dstTableName)
        : relMultiplicity{std::move(relMultiplicity)}, options(std::move(options)),
          srcTableName{std::move(srcTableName)}, dstTableName{std::move(dstTableName)} {}
};

struct ExtraCreateRelTableGroupInfo : public ExtraCreateTableInfo {
    std::string relMultiplicity;
    std::vector<std::pair<std::string, std::string>> srcDstTablePairs;

    ExtraCreateRelTableGroupInfo(std::string relMultiplicity,
        std::vector<std::pair<std::string, std::string>> srcDstTablePairs)
        : relMultiplicity{std::move(relMultiplicity)},
          srcDstTablePairs{std::move(srcDstTablePairs)} {}
};

} // namespace parser
} // namespace kuzu
