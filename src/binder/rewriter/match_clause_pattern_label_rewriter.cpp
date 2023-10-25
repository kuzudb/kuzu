#include "binder/rewriter/match_clause_pattern_label_rewriter.h"

#include "binder/query/reading_clause/bound_match_clause.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

void MatchClausePatternLabelRewriter::visitMatch(const BoundReadingClause& readingClause) {
    auto matchClause = reinterpret_cast<const BoundMatchClause&>(readingClause);
    if (matchClause.getMatchClauseType() == MatchClauseType::OPTIONAL_MATCH) {
        return;
    }
    auto collection = matchClause.getQueryGraphCollection();
    for (auto i = 0u; i < collection->getNumQueryGraphs(); ++i) {
        analyzer.pruneLabel(*collection->getQueryGraph(i));
    }
}

} // namespace binder
} // namespace kuzu
