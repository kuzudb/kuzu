#pragma once

#include "parser/parsed_statement_visitor.h"

namespace kuzu {
namespace parser {

class StandaloneCallAnalyzer final : public StatementVisitor {
public:
    explicit StandaloneCallAnalyzer(main::ClientContext* context)
        : StatementVisitor{context}, rewriteQuery{} {}

    std::string getRewriteQuery(const Statement& statement);

private:
    void visitStandaloneCallFunction(const Statement& statement) override;

private:
    std::string rewriteQuery;
};

} // namespace parser
} // namespace kuzu
