#include "include/unwind_clause.h"

namespace kuzu {
namespace parser {

bool UnwindClause::equals(const ReadingClause& other) const {
    auto& unwindClause = (UnwindClause&)other;
    if (!ReadingClause::equals(other)) {
        return false;
    }
    if (*unwindClause.getExpression() != *this->getExpression() ||
        this->getAlias() != unwindClause.getAlias()) {
        return false;
    }
    return true;
}

} // namespace parser
} // namespace kuzu
