#include "binder/expression/case_expression.h"
#include "binder/expression/function_expression.h"
#include "binder/expression_binder.h"
#include "function/rewrite_function.h"
#include "function/utility/vector_utility_functions.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::shared_ptr<Expression> rewriteFunc(const expression_vector& params,
    ExpressionBinder* binder) {
    KU_ASSERT(params.size() == 2);
    auto uniqueExpressionName =
        ScalarFunctionExpression::getUniqueName(NullIfFunction::name, params);
    auto resultType = params[0]->getDataType();
    auto caseExpression =
        std::make_shared<CaseExpression>(resultType, params[0], uniqueExpressionName);
    auto whenExpression = binder->bindComparisonExpression(ExpressionType::EQUALS, params);
    auto thenExpression = binder->createNullLiteralExpression();
    thenExpression = binder->implicitCastIfNecessary(thenExpression, resultType);
    caseExpression->addCaseAlternative(whenExpression, thenExpression);
    return caseExpression;
}

function_set NullIfFunction::getFunctionSet() {
    function_set functionSet;
    for (auto typeID : LogicalTypeUtils::getAllValidLogicTypes()) {
        functionSet.push_back(std::make_unique<RewriteFunction>(name,
            std::vector<LogicalTypeID>{typeID, typeID}, rewriteFunc));
    }
    return functionSet;
}

} // namespace function
} // namespace kuzu
