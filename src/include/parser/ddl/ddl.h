#pragma once

#include <string>

#include "parser/statement.h"

namespace kuzu {
namespace parser {

using namespace std;

class DDL : public Statement {
public:
    explicit DDL(StatementType statementType, string tableName)
        : Statement{statementType}, tableName{move(tableName)} {}

    inline string getTableName() const { return tableName; }

protected:
    string tableName;
};

} // namespace parser
} // namespace kuzu
