#include "include/query_part.h"

namespace kuzu {
namespace parser {

bool QueryPart::operator==(const QueryPart& other) const {
    if (readingClauses.size() != other.readingClauses.size() || *withClause != *other.withClause) {
        return false;
    }
    for (auto i = 0u; i < readingClauses.size(); ++i) {
        auto &thisReadingClause = *readingClauses[i], otherReadingClause = *other.readingClauses[i];
        if (thisReadingClause != otherReadingClause) {
            return false;
        }
    }
    return true;
}

} // namespace parser
} // namespace kuzu
