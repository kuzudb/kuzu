#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundDDL : public BoundStatement {
public:
    explicit BoundDDL(common::StatementType statementType, std::string tableName)
        : BoundStatement{statementType, BoundStatementResult::createSingleStringColumnResult()},
          tableName{std::move(tableName)} {}

    inline std::string getTableName() const { return tableName; }

private:
    std::string tableName;
};

} // namespace binder
} // namespace kuzu
