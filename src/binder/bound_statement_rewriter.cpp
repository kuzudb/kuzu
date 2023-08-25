#include "binder/bound_statement_rewriter.h"

#include "binder/rewriter/with_clause_projection_rewriter.h"

namespace kuzu {
namespace binder {

void BoundStatementRewriter::rewrite(BoundStatement& boundStatement) {
    auto withClauseProjectionRewriter = WithClauseProjectionRewriter();
    withClauseProjectionRewriter.visit(boundStatement);
}

} // namespace binder
} // namespace kuzu
