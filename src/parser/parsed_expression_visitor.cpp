#include "parser/parsed_expression_visitor.h"

#include "common/cast.h"
#include "parser/expression/parsed_case_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::vector<ParsedExpression*> ParsedExpressionChildrenVisitor::collectChildren(
    const kuzu::parser::ParsedExpression& expression) {
    switch (expression.getExpressionType()) {
    case ExpressionType::CASE_ELSE: {
        return collectCaseChildren(expression);
    }
    default: {
        std::vector<ParsedExpression*> parsedExpressions;
        parsedExpressions.reserve(expression.getNumChildren());
        for (auto& child : expression.children) {
            parsedExpressions.push_back(child.get());
        }
        return parsedExpressions;
    }
    }
}

void ParsedExpressionChildrenVisitor::setChild(kuzu::parser::ParsedExpression& expression,
    uint64_t idx, std::unique_ptr<ParsedExpression> expressionToSet) {
    switch (expression.getExpressionType()) {
    case ExpressionType::CASE_ELSE: {
        setCaseChild(expression, idx, std::move(expressionToSet));
    } break;
    default: {
        expression.children[idx] = std::move(expressionToSet);
    }
    }
}

std::vector<ParsedExpression*> ParsedExpressionChildrenVisitor::collectCaseChildren(
    const ParsedExpression& expression) {
    std::vector<ParsedExpression*> children;
    auto& parsedCaseExpr =
        ku_dynamic_cast<const ParsedExpression&, const ParsedCaseExpression&>(expression);
    if (parsedCaseExpr.getCaseExpression() != nullptr) {
        children.push_back(parsedCaseExpr.getCaseExpression());
    }
    for (auto i = 0u; i < parsedCaseExpr.getNumCaseAlternative(); i++) {
        auto caseAlternative = parsedCaseExpr.getCaseAlternative(i);
        children.push_back(caseAlternative->whenExpression.get());
        children.push_back(caseAlternative->thenExpression.get());
    }
    if (parsedCaseExpr.getElseExpression() != nullptr) {
        children.push_back(parsedCaseExpr.getElseExpression());
    }
    return children;
}

void ParsedExpressionChildrenVisitor::setCaseChild(kuzu::parser::ParsedExpression& expression,
    uint64_t idx, std::unique_ptr<ParsedExpression> expressionToSet) {
    auto& parsedCaseExpr = ku_dynamic_cast<ParsedExpression&, ParsedCaseExpression&>(expression);
    if (idx == 0) {
        parsedCaseExpr.caseExpression = std::move(expressionToSet);
    } else if (idx < 1 + parsedCaseExpr.getNumCaseAlternative() * 2) {
        auto caseAlternativeIdx = (idx - 1) / 2;
        auto caseAlternative = parsedCaseExpr.getCaseAlternativeUnsafe(caseAlternativeIdx);
        if (idx % 2 == 0) {
            caseAlternative->thenExpression = std::move(expressionToSet);
        } else {
            caseAlternative->whenExpression = std::move(expressionToSet);
        }
    } else {
        parsedCaseExpr.elseExpression = std::move(expressionToSet);
    }
}

std::unique_ptr<ParsedExpression> MacroParameterReplacer::visit(
    std::unique_ptr<ParsedExpression> macroExpression) const {
    if (expressionNamesToReplace.contains(macroExpression->getRawName())) {
        return expressionNamesToReplace.at(macroExpression->getRawName())->copy();
    }
    auto children = ParsedExpressionChildrenVisitor::collectChildren(*macroExpression);
    for (auto i = 0u; i < children.size(); i++) {
        ParsedExpressionChildrenVisitor::setChild(*macroExpression, i, visit(children[i]->copy()));
    }
    return macroExpression;
}

} // namespace parser
} // namespace kuzu
