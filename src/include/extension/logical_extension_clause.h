#pragma once

#include "planner/operator/simple/logical_simple.h"

namespace kuzu {
namespace extension {

class LogicalExtensionClause : public planner::LogicalSimple {
    static constexpr planner::LogicalOperatorType type_ =
        planner::LogicalOperatorType::EXTENSION_CLAUSE;

public:
    explicit LogicalExtensionClause(std::string statementName)
        : LogicalSimple{type_}, statementName{std::move(statementName)} {}

    std::string getStatementName() const { return statementName; }

private:
    std::string statementName;
};

} // namespace extension
} // namespace kuzu
