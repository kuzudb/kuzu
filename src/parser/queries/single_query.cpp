#include "src/parser/include/queries/single_query.h"

namespace graphflow {
namespace parser {

bool SingleQuery::hasMatchStatement() const {
    for (auto& queryPart : queryParts) {
        if (queryPart->hasMatchStatement()) {
            return true;
        }
    }
    return !matchStatements.empty();
}

MatchStatement* SingleQuery::getFirstMatchStatement() const {
    assert(hasMatchStatement());
    for (auto& queryPart : queryParts) {
        if (queryPart->hasMatchStatement()) {
            return queryPart->getFirstMatchStatement();
        }
    }
    return matchStatements[0].get();
}

} // namespace parser
} // namespace graphflow
