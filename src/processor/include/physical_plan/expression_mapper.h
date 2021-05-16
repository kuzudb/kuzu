#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/physical_plan/operator/physical_operator_info.h"
#include "src/processor/include/physical_plan/operator/result/result_set.h"

using namespace graphflow::binder;
using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class ExpressionMapper {

public:
    static unique_ptr<ExpressionEvaluator> mapToPhysical(const Expression& expression,
        PhysicalOperatorsInfo& physicalOperatorInfo, ResultSet& resultSet);

    static unique_ptr<ExpressionEvaluator> clone(
        const ExpressionEvaluator& expression, ResultSet& resultSet);
};

} // namespace processor
} // namespace graphflow
