#pragma once

#include "parser/ddl/create_table.h"

namespace kuzu {
namespace parser {

class CreateRelClause : public CreateTable {
public:
    explicit CreateRelClause(std::string tableName,
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes,
        std::string relMultiplicity,
        std::vector<std::pair<std::string, std::string>> relConnections)
        : CreateTable{common::StatementType::CREATE_REL_CLAUSE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          relMultiplicity{std::move(relMultiplicity)}, relConnections{std::move(relConnections)} {}

    inline std::string getRelMultiplicity() const { return relMultiplicity; }

    inline std::vector<std::pair<std::string, std::string>> getRelConnections() const {
        return relConnections;
    }

private:
    std::string relMultiplicity;
    std::vector<std::pair<std::string, std::string>> relConnections;
};

} // namespace parser
} // namespace kuzu
