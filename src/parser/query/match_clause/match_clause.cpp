#include "include/match_clause.h"

namespace graphflow {
namespace parser {

bool MatchClause::operator==(const MatchClause& other) const {
    if (patternElements.size() != other.patternElements.size() ||
        hasWhereClause() != other.hasWhereClause() || isOptional != other.isOptional) {
        return false;
    }
    if (hasWhereClause() && *whereClause != *other.whereClause) {
        return false;
    }
    for (auto i = 0u; i < patternElements.size(); ++i) {
        if (*patternElements[i] != *other.patternElements[i]) {
            return false;
        }
    }
    return true;
}

} // namespace parser
} // namespace graphflow
