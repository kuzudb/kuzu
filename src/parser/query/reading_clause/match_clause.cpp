#include "include/match_clause.h"

namespace kuzu {
namespace parser {

bool MatchClause::equals(const ReadingClause& other) const {
    auto& matchClause = (MatchClause&)other;
    if (!ReadingClause::equals(other)) {
        return false;
    }
    if (patternElements.size() != matchClause.getPatternElements().size() ||
        hasWhereClause() != matchClause.hasWhereClause() || isOptional != matchClause.isOptional) {
        return false;
    }
    if (hasWhereClause() && *whereClause != *matchClause.whereClause) {
        return false;
    }
    for (auto i = 0u; i < patternElements.size(); ++i) {
        if (*patternElements[i] != *matchClause.patternElements[i]) {
            return false;
        }
    }
    return true;
}

} // namespace parser
} // namespace kuzu
