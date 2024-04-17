#pragma once

#include <string>

#include "parser/database_statement.h"

namespace kuzu {
namespace parser {

class DetachDatabase final : public DatabaseStatement {
public:
    explicit DetachDatabase(std::string dbName)
        : DatabaseStatement{common::StatementType::DETACH_DATABASE, std::move(dbName)} {}
};

} // namespace parser
} // namespace kuzu
