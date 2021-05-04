#pragma once

#include "src/expression/include/logical/logical_expression.h"
#include "src/expression/include/physical/physical_expression.h"
#include "src/processor/include/physical_plan/operator/physical_operator_info.h"
#include "src/processor/include/physical_plan/operator/result/result_set.h"

using namespace graphflow::expression;

namespace graphflow {
namespace processor {

class ExpressionMapper {

public:
    static unique_ptr<PhysicalExpression> mapToPhysical(const LogicalExpression& expression,
        PhysicalOperatorsInfo& physicalOperatorInfo, ResultSet& resultSet);

    static unique_ptr<PhysicalExpression> clone(
        const PhysicalExpression& expression, ResultSet& resultSet);
};

} // namespace processor
} // namespace graphflow
