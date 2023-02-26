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

std::string CaseExpression::toString() const {
    std::string result = "CASE ";
    for (auto& caseAlternative : caseAlternatives) {
        result += "WHEN " + caseAlternative->whenExpression->toString() + " THEN " +
                  caseAlternative->thenExpression->toString();
    }
    result += " ELSE " + elseExpression->toString();
    return result;
}

} // namespace binder
} // namespace kuzu
