#pragma once

#include <string>

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class DDL : public Statement {
public:
    explicit DDL(common::StatementType statementType, std::string tableName)
        : Statement{statementType}, tableName{std::move(tableName)} {}

    inline std::string getTableName() const { return tableName; }

protected:
    std::string tableName;
};

} // namespace parser
} // namespace kuzu
