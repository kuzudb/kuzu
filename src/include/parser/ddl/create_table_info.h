#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/copy_constructors.h"
#include "common/enums/conflict_action.h"
#include "common/enums/table_type.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

struct ExtraCreateTableInfo {
    virtual ~ExtraCreateTableInfo() = default;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ExtraCreateTableInfo&, const TARGET&>(*this);
    }
};

struct PropertyDefinition {
    std::string name;
    std::string type;

    PropertyDefinition(std::string name, std::string type)
        : name{std::move(name)}, type{std::move(type)} {}
    DELETE_COPY_DEFAULT_MOVE(PropertyDefinition);
};

struct PropertyDefinitionDDL : public PropertyDefinition {
    std::unique_ptr<ParsedExpression> expr;

    PropertyDefinitionDDL(std::string name, std::string type,
        std::unique_ptr<ParsedExpression> expr)
        : PropertyDefinition{name, type}, expr{std::move(expr)} {}
    DELETE_COPY_DEFAULT_MOVE(PropertyDefinitionDDL);
};

struct CreateTableInfo {
    common::TableType tableType;
    std::string tableName;
    std::vector<PropertyDefinitionDDL> propertyDefinitions;
    std::unique_ptr<ExtraCreateTableInfo> extraInfo;
    common::ConflictAction onConflict;

    CreateTableInfo(common::TableType tableType, std::string tableName,
        common::ConflictAction onConflict)
        : tableType{tableType}, tableName{std::move(tableName)}, extraInfo{nullptr},
          onConflict{onConflict} {}
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

    ExtraCreateRelTableInfo(std::string relMultiplicity, std::string srcTableName,
        std::string dstTableName)
        : relMultiplicity{std::move(relMultiplicity)}, srcTableName{std::move(srcTableName)},
          dstTableName{std::move(dstTableName)} {}
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
