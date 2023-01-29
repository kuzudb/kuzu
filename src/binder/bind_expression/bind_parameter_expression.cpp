#include "binder/expression/parameter_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_parameter_expression.h"

namespace kuzu {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindParameterExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedParameterExpression = (ParsedParameterExpression&)parsedExpression;
    auto parameterName = parsedParameterExpression.getParameterName();
    if (parameterMap.contains(parameterName)) {
        return make_shared<ParameterExpression>(parameterName, parameterMap.at(parameterName));
    } else {
        auto value = make_shared<Value>(Value::createNullValue());
        parameterMap.insert({parameterName, value});
        return make_shared<ParameterExpression>(parameterName, value);
    }
}

} // namespace binder
} // namespace kuzu
