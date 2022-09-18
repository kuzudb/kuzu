#include "src/parser/query/include/single_query.h"

namespace graphflow {
namespace parser {

bool SingleQuery::isFirstReadingClauseOptionalMatch() const {
    for (auto& queryPart : queryParts) {
        if (queryPart->getNumReadingClauses() != 0 &&
            queryPart->getReadingClause(0)->getClauseType() == ClauseType::MATCH) {
            return ((MatchClause*)queryPart->getReadingClause(0))->getIsOptional();
        } else {
            return false;
        }
    }
    if (getNumReadingClauses() != 0 && getReadingClause(0)->getClauseType() == ClauseType::MATCH) {
        return ((MatchClause*)getReadingClause(0))->getIsOptional();
    }
    return false;
}

bool SingleQuery::operator==(const SingleQuery& other) const {
    if (queryParts.size() != other.queryParts.size() ||
        readingClauses.size() != other.readingClauses.size() ||
        *returnClause != *other.returnClause) {
        return false;
    }
    for (auto i = 0u; i < queryParts.size(); ++i) {
        if (*queryParts[i] != *other.queryParts[i]) {
            return false;
        }
    }
    for (auto i = 0u; i < readingClauses.size(); ++i) {
        if (*readingClauses[i] != *other.readingClauses[i]) {
            return false;
        }
    }
    return true;
}

} // namespace parser
} // namespace graphflow
