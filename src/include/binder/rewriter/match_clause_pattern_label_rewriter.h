#pragma once

#include "binder/bound_statement_visitor.h"
#include "binder/query/query_graph_label_analyzer.h"

namespace kuzu {
namespace binder {

class MatchClausePatternLabelRewriter : public BoundStatementVisitor {
public:
    // TODO(Jiamin): remove catalog
    explicit MatchClausePatternLabelRewriter(
        const catalog::Catalog& catalog, const main::ClientContext& clientContext)
        : analyzer{catalog, clientContext} {}

    void visitMatch(const BoundReadingClause& readingClause) final;

private:
    QueryGraphLabelAnalyzer analyzer;
};

} // namespace binder
} // namespace kuzu
