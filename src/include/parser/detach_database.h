#pragma once

#include <string>

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class DetachDatabase final : public Statement {
public:
    explicit DetachDatabase(std::string dbName)
        : Statement{common::StatementType::DETACH_DATABASE}, dbName{std::move(dbName)} {}

    std::string getDBName() const { return dbName; }

private:
    std::string dbName;
};

} // namespace parser
} // namespace kuzu
