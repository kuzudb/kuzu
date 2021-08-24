#include "src/binder/include/bound_queries/bound_single_query.h"

#include "src/binder/include/bound_statements/bound_match_statement.h"

namespace graphflow {
namespace binder {

uint32_t BoundSingleQuery::getNumQueryRels() const {
    auto numQueryRels = 0u;
    for (auto& boundQueryPart : boundQueryParts) {
        numQueryRels += boundQueryPart->getNumQueryRels();
    }
    for (auto& boundReadingStatement : boundReadingStatements) {
        if (MATCH_STATEMENT == boundReadingStatement->statementType) {
            numQueryRels +=
                ((BoundMatchStatement&)*boundReadingStatement).queryGraph->getNumQueryRels();
        }
    }
    return numQueryRels;
}

vector<shared_ptr<Expression>> BoundSingleQuery::getIncludedVariables() const {
    vector<shared_ptr<Expression>> result;
    for (auto& queryPart : boundQueryParts) {
        for (auto& variable : queryPart->getIncludedVariables()) {
            result.push_back(variable);
        }
    }
    for (auto& readingStatement : boundReadingStatements) {
        for (auto& variable : readingStatement->getIncludedVariables()) {
            result.push_back(variable);
        }
    }
    for (auto& variable : boundReturnStatement->getIncludedVariables()) {
        result.push_back(variable);
    }
    return result;
}

} // namespace binder
} // namespace graphflow
