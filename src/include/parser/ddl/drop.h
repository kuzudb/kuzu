#pragma once

#include <string>

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Drop : public Statement {
public:
    explicit Drop(std::string tableName)
        : Statement{common::StatementType::DROP_TABLE}, tableName{std::move(tableName)} {}

    inline std::string getTableName() const { return tableName; }

private:
    std::string tableName;
};

} // namespace parser
} // namespace kuzu
