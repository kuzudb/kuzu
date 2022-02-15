#include "test/test_utility/include/aggregate_function_test_helper.h"

#include "src/binder/expression/include/function_expression.h"

using namespace graphflow::function;

namespace graphflow {
namespace testing {

unique_ptr<AggregateFunction> AggregateFunctionTestHelper::getAggregateFunction(
    ExpressionType expressionType, DataType childDataType) {
    auto expression = make_unique<FunctionExpression>(
        expressionType, INT64 /* dummy data type*/, getDummyExpression(childDataType));
    return AggregateFunctionUtil::getAggregateFunction(*expression);
}

shared_ptr<Expression> AggregateFunctionTestHelper::getDummyExpression(DataType dataType) {
    return make_unique<Expression>(
        PROPERTY /* dummy expression type */, dataType, "dummy" /* dummy expression name*/);
}

} // namespace testing
} // namespace graphflow
