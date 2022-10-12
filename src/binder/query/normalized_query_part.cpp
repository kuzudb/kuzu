#include "include/normalized_query_part.h"

#include "reading_clause/include/bound_match_clause.h"

namespace graphflow {
namespace binder {

expression_vector NormalizedQueryPart::getPropertiesToRead() const {
    expression_vector result;
    for (auto i = 0u; i < getNumReadingClause(); i++) {
        if (getReadingClause(i)->getClauseType() == ClauseType::MATCH) {
            auto boundMatchClause = (BoundMatchClause*)getReadingClause(i);
            if (boundMatchClause->getWhereExpression() != nullptr) {
                for (auto& property :
                    boundMatchClause->getWhereExpression()->getSubPropertyExpressions()) {
                    result.push_back(property);
                }
            }
        } else if (getReadingClause(i)->getClauseType() == ClauseType::UNWIND) {
            auto boundUnwindClause = (BoundUnwindClause*)getReadingClause(i);
            if (boundUnwindClause->getExpression() != nullptr) {
                for (auto& property :
                    boundUnwindClause->getExpression()->getSubPropertyExpressions()) {
                    result.push_back(property);
                }
            }
        } else {
            assert(false);
        }
    }
    for (auto& updatingClause : updatingClauses) {
        for (auto& property : updatingClause->getPropertiesToRead()) {
            result.push_back(property);
        }
    }
    if (hasProjectionBody()) {
        for (auto& property : projectionBody->getPropertiesToRead()) {
            result.push_back(property);
        }
        if (hasProjectionBodyPredicate()) {
            for (auto& property : projectionBodyPredicate->getSubPropertyExpressions()) {
                result.push_back(property);
            }
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
