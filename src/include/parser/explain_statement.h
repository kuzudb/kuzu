#pragma once

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class ExplainStatement : public Statement {
public:
    explicit ExplainStatement(std::unique_ptr<Statement> statementToExplain)
        : Statement{common::StatementType::EXPLAIN}, statementToExplain{
                                                         std::move(statementToExplain)} {}

    inline Statement* getStatementToExplain() const { return statementToExplain.get(); }

private:
    std::unique_ptr<Statement> statementToExplain;
};

} // namespace parser
} // namespace kuzu
