#include "binder/bound_statement_rewriter.h"

#include "binder/rewriter/match_clause_pattern_label_rewriter.h"
#include "binder/rewriter/with_clause_projection_rewriter.h"

namespace kuzu {
namespace binder {

void BoundStatementRewriter::rewrite(
    BoundStatement& boundStatement, const catalog::Catalog& catalog) {
    auto withClauseProjectionRewriter = WithClauseProjectionRewriter();
    withClauseProjectionRewriter.visit(boundStatement);

    auto matchClausePatternLabelRewriter = MatchClausePatternLabelRewriter(catalog);
    matchClausePatternLabelRewriter.visit(boundStatement);
}

} // namespace binder
} // namespace kuzu
