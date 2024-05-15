#include "binder/bound_statement_rewriter.h"

#include "binder/rewriter/match_clause_pattern_label_rewriter.h"
#include "binder/rewriter/with_clause_projection_rewriter.h"
#include "binder/visitor/default_type_solver.h"

namespace kuzu {
namespace binder {

void BoundStatementRewriter::rewrite(BoundStatement& boundStatement,
    const main::ClientContext& clientContext) {
    auto withClauseProjectionRewriter = WithClauseProjectionRewriter();
    withClauseProjectionRewriter.visitUnsafe(boundStatement);

    auto matchClausePatternLabelRewriter = MatchClausePatternLabelRewriter(clientContext);
    matchClausePatternLabelRewriter.visitUnsafe(boundStatement);

    auto defaultTypeSolver = DefaultTypeSolver();
    defaultTypeSolver.visit(boundStatement);
}

} // namespace binder
} // namespace kuzu
