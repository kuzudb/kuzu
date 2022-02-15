#include "include/with_clause.h"

namespace graphflow {
namespace parser {

bool WithClause::operator==(const WithClause& other) const {
    if (ReturnClause::operator!=(other) || hasWhereExpression() != other.hasWhereExpression()) {
        return false;
    }
    if (hasWhereExpression() && *whereExpression != *other.whereExpression) {
        return false;
    }
    return true;
}

} // namespace parser
} // namespace graphflow
