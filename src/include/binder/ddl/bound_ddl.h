#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundDDL : public BoundStatement {
public:
    explicit BoundDDL(StatementType statementType, string tableName)
        : BoundStatement{statementType, BoundStatementResult::createSingleStringColumnResult()},
          tableName{std::move(tableName)} {}

    inline string getTableName() const { return tableName; }

private:
    string tableName;
};

} // namespace binder
} // namespace kuzu
