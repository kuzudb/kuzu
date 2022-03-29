#pragma once

#include "src/function/include/aggregate/aggregate_function.h"

using namespace graphflow::function;

namespace graphflow {
namespace testing {

class AggregateFunctionTestHelper {

public:
    static unique_ptr<AggregateFunction> getAggregateFunction(
        ExpressionType expressionType, DataTypeID childDataType);

private:
    static shared_ptr<Expression> getDummyExpression(DataTypeID dataTypeID);
};

} // namespace testing
} // namespace graphflow
