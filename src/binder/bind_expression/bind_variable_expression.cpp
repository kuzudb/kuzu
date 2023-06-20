#include "binder/binder.h"
#include "binder/expression/variable_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_variable_expression.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto& variableExpression = (ParsedVariableExpression&)parsedExpression;
    auto variableName = variableExpression.getVariableName();
    if (binder->variableScope->contains(variableName)) {
        return binder->variableScope->getExpression(variableName);
    }
    throw common::BinderException(
        "Variable " + parsedExpression.getRawName() + " is not in scope.");
}

std::shared_ptr<Expression> ExpressionBinder::createVariableExpression(
    common::LogicalType logicalType, std::string uniqueName, std::string name) {
    return std::make_shared<VariableExpression>(
        std::move(logicalType), std::move(uniqueName), std::move(name));
}

} // namespace binder
} // namespace kuzu
