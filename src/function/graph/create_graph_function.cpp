#include "binder/expression/expression_util.h"
#include "binder/expression/graph_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "function/graph/graph_functions.h"
#include "function/rewrite_function.h"
#include "main/client_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::shared_ptr<Expression> rewriteFunc(const expression_vector& params,
    ExpressionBinder* expressionBinder) {
    auto uniqueName = expressionBinder->getUniqueName(CreateGraphFunction::name);
    if (params.size() != 2) {
        throw BinderException("Graph function requires 2 parameters.");
    }
    ExpressionUtil::validateExpressionType(*params[0], ExpressionType::LITERAL);
    ExpressionUtil::validateExpressionType(*params[1], ExpressionType::LITERAL);
    ExpressionUtil::validateDataType(*params[0], *LogicalType::STRING());
    ExpressionUtil::validateDataType(*params[1], *LogicalType::STRING());
    auto nodeName =
        params[0]->constPtrCast<LiteralExpression>()->getValue().getValue<std::string>();
    auto relName = params[1]->constPtrCast<LiteralExpression>()->getValue().getValue<std::string>();
    return std::make_shared<GraphExpression>(std::move(uniqueName), nodeName, relName);
}

function_set CreateGraphFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<RewriteFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, rewriteFunc);
    function->isVarLength = true;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
