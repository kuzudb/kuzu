#pragma once

#include "extension_clause_handler.h"

namespace kuzu {
namespace extension {

class ExtensionClause : public parser::Statement {
public:
    static constexpr const common::StatementType TYPE = common::StatementType::EXTENSION_CLAUSE;

public:
    explicit ExtensionClause(ExtensionClauseHandler handler)
        : parser::Statement{TYPE}, handler{std::move(handler)} {}

    ExtensionClauseHandler getExtensionClauseHandler() const { return handler; }

private:
    ExtensionClauseHandler handler;
};

class BoundExtensionClause : public binder::BoundStatement {
public:
    static constexpr const common::StatementType TYPE = common::StatementType::EXTENSION_CLAUSE;

public:
    BoundExtensionClause(ExtensionClauseHandler handler,
        binder::BoundStatementResult boundStatementResult)
        : binder::BoundStatement{TYPE, std::move(boundStatementResult)},
          handler{std::move(handler)} {}

    ExtensionClauseHandler getExtensionClauseHandler() const { return handler; }

private:
    ExtensionClauseHandler handler;
};

class LogicalExtensionClause : public planner::LogicalOperator {
public:
    static constexpr const planner::LogicalOperatorType TYPE =
        planner::LogicalOperatorType::EXTENSION_CLAUSE;

public:
    explicit LogicalExtensionClause(ExtensionClauseHandler handler)
        : planner::LogicalOperator{TYPE}, handler{std::move(handler)} {}

    ExtensionClauseHandler getExtensionClauseHandler() const { return handler; }

private:
    ExtensionClauseHandler handler;
};

class PhysicalExtensionClause : public processor::PhysicalOperator {
public:
    PhysicalExtensionClause(uint32_t id, const std::string& paramsString)
        : processor::PhysicalOperator{processor::PhysicalOperatorType::EXTENSION_CLAUSE, id,
              std::make_unique<processor::OPPrintInfo>(paramsString)} {}
};

} // namespace extension
} // namespace kuzu
