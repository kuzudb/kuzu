#pragma once

#include "parser/ddl/create_table.h"

namespace kuzu {
namespace parser {

class CreateNodeClause : public CreateTable {
public:
    explicit CreateNodeClause(std::string tableName,
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes,
        std::string pkColName)
        : CreateTable{common::StatementType::CREATE_NODE_CLAUSE, std::move(tableName),
              std::move(propertyNameDataTypes)},
          pKColName{std::move(pkColName)} {}

    inline std::string getPKColName() const { return pKColName; }

private:
    std::string pKColName;
};

} // namespace parser
} // namespace kuzu
