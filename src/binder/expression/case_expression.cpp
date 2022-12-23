#include "binder/expression/case_expression.h"

namespace kuzu {
namespace binder {

expression_vector CaseExpression::getChildren() const {
    expression_vector result;
    for (auto& caseAlternative : caseAlternatives) {
        result.push_back(caseAlternative->whenExpression);
        result.push_back(caseAlternative->thenExpression);
    }
    result.push_back(elseExpression);
    return result;
}

} // namespace binder
} // namespace kuzu
