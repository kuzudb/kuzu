#pragma once

#include "binder/bound_database_statement.h"

namespace kuzu {
namespace binder {

class BoundUseDatabase final : public BoundDatabaseStatement {
public:
    explicit BoundUseDatabase(std::string dbName)
        : BoundDatabaseStatement{common::StatementType::USE_DATABASE, std::move(dbName)} {}
};

} // namespace binder
} // namespace kuzu
