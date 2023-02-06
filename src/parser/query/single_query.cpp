#include "parser/query/single_query.h"

namespace kuzu {
namespace parser {

bool SingleQuery::isFirstReadingClauseOptionalMatch() const {
    for (auto& queryPart : queryParts) {
        if (queryPart->getNumReadingClauses() != 0 &&
            queryPart->getReadingClause(0)->getClauseType() == common::ClauseType::MATCH) {
            return ((MatchClause*)queryPart->getReadingClause(0))->getIsOptional();
        } else {
            return false;
        }
    }
    if (getNumReadingClauses() != 0 &&
        getReadingClause(0)->getClauseType() == common::ClauseType::MATCH) {
        return ((MatchClause*)getReadingClause(0))->getIsOptional();
    }
    return false;
}

} // namespace parser
} // namespace kuzu
