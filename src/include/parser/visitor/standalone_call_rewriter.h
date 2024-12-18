#pragma once

#include "parser/parsed_statement_visitor.h"

namespace kuzu {
namespace function {
class TableFunction;
class TableFuncBindData;
} // namespace function

namespace parser {

class StandaloneCallRewriter final : public StatementVisitor {
public:
    explicit StandaloneCallRewriter(main::ClientContext* context)
        : StatementVisitor{}, rewriteQuery{}, context{context} {}

    std::string getRewriteQuery(const Statement& statement);

private:
    void rewrite(const function::TableFunction& tableFunc,
        const function::TableFuncBindData& bindData);

    void visitStandaloneCallFunction(const Statement& statement) override;

    void visitInQueryCall(const ReadingClause* /*readingClause*/) override;

private:
    std::string rewriteQuery;
    main::ClientContext* context;
};

} // namespace parser
} // namespace kuzu
