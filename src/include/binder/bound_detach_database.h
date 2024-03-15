#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundDetachDatabase final : public BoundStatement {
public:
    explicit BoundDetachDatabase(std::string dbName)
        : BoundStatement{common::StatementType::DETACH_DATABASE,
              BoundStatementResult::createEmptyResult()},
          dbName{std::move(dbName)} {}

    std::string getDBName() const { return dbName; }

private:
    std::string dbName;
};

} // namespace binder
} // namespace kuzu
