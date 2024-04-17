#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundDatabaseStatement : public BoundStatement {
public:
    explicit BoundDatabaseStatement(common::StatementType statementType, std::string dbName)
        : BoundStatement{statementType, BoundStatementResult::createSingleStringColumnResult()},
          dbName{std::move(dbName)} {}

    std::string getDBName() const { return dbName; }

private:
    std::string dbName;
};

} // namespace binder
} // namespace kuzu
