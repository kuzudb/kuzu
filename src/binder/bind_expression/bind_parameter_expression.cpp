#include "binder/expression/parameter_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_parameter_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindParameterExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedParameterExpression = parsedExpression.constCast<ParsedParameterExpression>();
    auto parameterName = parsedParameterExpression.getParameterName();
    parsedParameters.insert(parameterName);
    if (parameterMap.contains(parameterName)) {
        return make_shared<ParameterExpression>(parameterName, *parameterMap.at(parameterName));
    } else {
        auto value = std::make_shared<Value>(Value::createNullValue());
        parameterMap.insert({parameterName, value});
        return std::make_shared<ParameterExpression>(parameterName, *value);
    }
}

} // namespace binder
} // namespace kuzu
