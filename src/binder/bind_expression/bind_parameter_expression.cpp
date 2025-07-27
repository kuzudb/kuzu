#include "binder/expression/parameter_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_parameter_expression.h"
#include <common/exception/runtime.h>

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindParameterExpression(
    const ParsedExpression& parsedExpression) {
    auto& parsedParameterExpression = parsedExpression.constCast<ParsedParameterExpression>();
    auto parameterName = parsedParameterExpression.getParameterName();
    if (parameterMap.contains(parameterName)) {
        return std::make_shared<ParameterExpression>(parameterName, parameterMap.at(parameterName));
    }
    throw RuntimeException("TODO: fill me");
}

} // namespace binder
} // namespace kuzu
