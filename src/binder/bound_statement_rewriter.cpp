#include "binder/bound_statement_rewriter.h"

#include "binder/rewriter/match_clause_pattern_label_rewriter.h"
#include "binder/rewriter/with_clause_projection_rewriter.h"
#include "binder/visitor/default_type_solver.h"

namespace kuzu {
namespace binder {

void BoundStatementRewriter::rewrite(
    BoundStatement& boundStatement, const catalog::Catalog& catalog) {
    auto withClauseProjectionRewriter = WithClauseProjectionRewriter();
    withClauseProjectionRewriter.visitUnsafe(boundStatement);

    auto matchClausePatternLabelRewriter = MatchClausePatternLabelRewriter(catalog);
    matchClausePatternLabelRewriter.visit(boundStatement);

    auto defaultTypeSolver = DefaultTypeSolver();
    defaultTypeSolver.visit(boundStatement);
}

} // namespace binder
} // namespace kuzu
