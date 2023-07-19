#include "parser/expression/parsed_case_expression.h"

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedExpression> ParsedCaseExpression::copy() const {
    std::vector<std::unique_ptr<ParsedCaseAlternative>> caseAlternativesCopy;
    caseAlternativesCopy.reserve(caseAlternatives.size());
    for (auto& caseAlternative : caseAlternatives) {
        caseAlternativesCopy.push_back(caseAlternative->copy());
    }
    return std::make_unique<ParsedCaseExpression>(type, alias, rawName, copyChildren(),
        caseExpression->copy(), std::move(caseAlternativesCopy), elseExpression->copy());
}

} // namespace parser
} // namespace kuzu
