#pragma once

#include "parser/parsed_statement_visitor.h"

namespace kuzu {
namespace parser {

class StandaloneCallRewriter final : public StatementVisitor {
public:
    explicit StandaloneCallRewriter(main::ClientContext* context)
        : StatementVisitor{}, rewriteQuery{}, context{context} {}

    std::string getRewriteQuery(const Statement& statement);

private:
    void visitStandaloneCallFunction(const Statement& statement) override;

private:
    std::string rewriteQuery;
    main::ClientContext* context;
};

} // namespace parser
} // namespace kuzu
