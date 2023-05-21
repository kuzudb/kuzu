#pragma once

#include "parser/ddl/create_table.h"

namespace kuzu {
namespace parser {

class CreateRelClause : public CreateTable {
public:
    CreateRelClause(std::string tableName,
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes,
        std::string relMultiplicity, std::string srcTableName, std::string dstTableName)
        : CreateTable{common::StatementType::CREATE_REL_TABLE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          relMultiplicity{std::move(relMultiplicity)}, srcTableName{std::move(srcTableName)},
          dstTableName{std::move(dstTableName)} {}

    inline std::string getRelMultiplicity() const { return relMultiplicity; }

    inline std::string getSrcTableName() const { return srcTableName; }

    inline std::string getDstTableName() const { return dstTableName; }

private:
    std::string relMultiplicity;
    std::string srcTableName;
    std::string dstTableName;
};

} // namespace parser
} // namespace kuzu
