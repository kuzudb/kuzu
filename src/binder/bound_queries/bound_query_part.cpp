#include "src/binder/include/bound_queries/bound_query_part.h"

#include "src/binder/include/bound_statements/bound_match_statement.h"

namespace graphflow {
namespace binder {

uint32_t BoundQueryPart::getNumQueryRels() const {
    for (auto& boundReadingStatement : boundReadingStatements) {
        if (MATCH_STATEMENT == boundReadingStatement->statementType) {
            // Query part has at most one match statement because we merged multiple match
            // statements in binder.
            return ((BoundMatchStatement&)*boundReadingStatement).queryGraph->getNumQueryRels();
        }
    }
    return 0;
}

vector<const Expression*> BoundQueryPart::getIncludedVariables() const {
    vector<const Expression*> result;
    for (auto& readingStatement : boundReadingStatements) {
        for (auto& variable : readingStatement->getIncludedVariables()) {
            result.push_back(variable);
        }
    }
    for (auto& variable : boundWithStatement->getIncludedVariables()) {
        result.push_back(variable);
    }
    return result;
}

} // namespace binder
} // namespace graphflow
