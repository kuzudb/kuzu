#include "include/query_part.h"

namespace graphflow {
namespace parser {

bool QueryPart::operator==(const QueryPart& other) const {
    if (matchClauses.size() != other.matchClauses.size() || *withClause != *other.withClause) {
        return false;
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
