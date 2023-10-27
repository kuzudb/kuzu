#pragma once

#include "binder/bound_statement_visitor.h"
#include "binder/query/query_graph_label_analyzer.h"

namespace kuzu {
namespace binder {

class MatchClausePatternLabelRewriter : public BoundStatementVisitor {
public:
    MatchClausePatternLabelRewriter(const catalog::Catalog& catalog) : analyzer{catalog} {}

    void visitMatch(const BoundReadingClause& readingClause) final;

private:
    QueryGraphLabelAnalyzer analyzer;
};

} // namespace binder
} // namespace kuzu
