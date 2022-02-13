#include "src/parser/query/include/single_query.h"

namespace graphflow {
namespace parser {

bool SingleQuery::isFirstMatchOptional() const {
    for (auto& queryPart : queryParts) {
        if (queryPart->getNumMatchClauses() != 0) {
            return queryPart->getMatchClause(0)->getIsOptional();
        }
    }
    if (!matchClauses.empty()) {
        return matchClauses[0]->getIsOptional();
    }
    return false;
}

bool SingleQuery::operator==(const SingleQuery& other) const {
    if (queryParts.size() != other.queryParts.size() ||
        matchClauses.size() != other.matchClauses.size() || *returnClause != *other.returnClause) {
        return false;
    }
    for (auto i = 0u; i < queryParts.size(); ++i) {
        if (*queryParts[i] != *other.queryParts[i]) {
            return false;
        }
    }
    for (auto i = 0u; i < matchClauses.size(); ++i) {
        if (*matchClauses[i] != *other.matchClauses[i]) {
            return false;
        }
    }
    return true;
}

} // namespace parser
} // namespace graphflow
