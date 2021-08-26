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

vector<shared_ptr<Expression>> BoundQueryPart::getDependentProperties() const {
    auto result = boundWithStatement->getDependentProperties();
    for (auto& readingStatement : boundReadingStatements) {
        for (auto& propertyExpression : readingStatement->getDependentProperties()) {
            result.push_back(propertyExpression);
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
