#include "binder/binder.h"
#include "binder/expression/variable_expression.h"
#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "parser/expression/parsed_variable_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto& variableExpression = (ParsedVariableExpression&)parsedExpression;
    auto variableName = variableExpression.getVariableName();
    if (binder->scope->contains(variableName)) {
        return binder->scope->getExpression(variableName);
    }
    throw BinderException("Variable " + parsedExpression.getRawName() + " is not in scope.");
}

std::shared_ptr<Expression> ExpressionBinder::createVariableExpression(
    LogicalType logicalType, std::string name) {
    return std::make_shared<VariableExpression>(
        std::move(logicalType), binder->getUniqueExpressionName(name), std::move(name));
}

} // namespace binder
} // namespace kuzu
