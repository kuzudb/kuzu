#pragma once

#include <string>

#include "src/parser/include/statement.h"

namespace graphflow {
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
} // namespace graphflow
