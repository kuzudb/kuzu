#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/variable_expression.h"
#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "main/client_context.h"
#include "parser/expression/parsed_variable_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindVariableExpression(
    const ParsedExpression& parsedExpression) {
    auto& variableExpression =
        ku_dynamic_cast<const ParsedExpression&, const ParsedVariableExpression&>(parsedExpression);
    auto variableName = variableExpression.getVariableName();
    return bindVariableExpression(variableName);
}

std::shared_ptr<Expression> ExpressionBinder::bindVariableExpression(const std::string& varName) {
    if (binder->scope->contains(varName)) {
        return binder->scope->getExpression(varName);
    }
    if (binder->clientContext->hasReplaceFunc()) {
        auto val = Value(varName);
        auto replacedVal = binder->clientContext->replaceFunc(&val);
        if (replacedVal != nullptr) {
            return std::make_shared<LiteralExpression>(
                replacedVal->copy(), binder->getUniqueExpressionName(replacedVal->toString()));
        }
    }
    throw BinderException(stringFormat("Variable {} is not in scope.", varName));
}

std::shared_ptr<Expression> ExpressionBinder::createVariableExpression(
    common::LogicalType logicalType, std::string_view name) {
    return createVariableExpression(logicalType, std::string(name));
}

std::shared_ptr<Expression> ExpressionBinder::createVariableExpression(
    LogicalType logicalType, std::string name) {
    return std::make_shared<VariableExpression>(
        std::move(logicalType), binder->getUniqueExpressionName(name), std::move(name));
}

} // namespace binder
} // namespace kuzu
