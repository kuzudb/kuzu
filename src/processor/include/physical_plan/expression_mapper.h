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
    static unique_ptr<ExpressionEvaluator> mapToPhysical(MemoryManager& memoryManager,
        const Expression& expression, PhysicalOperatorsInfo& physicalOperatorInfo,
        ResultSet& resultSet, bool isSelectOperation);

    static unique_ptr<ExpressionEvaluator> clone(MemoryManager& memoryManager,
        const ExpressionEvaluator& expression, ResultSet& resultSet, bool isSelectOperation);

private:
    static unique_ptr<ExpressionEvaluator> mapChildExpressionAndCastToUnstructuredIfNecessary(
        MemoryManager& memoryManager, const Expression& expression, bool castToUnstructured,
        PhysicalOperatorsInfo& physicalOperatorInfo, ResultSet& resultSet, bool isSelectOperation);
};

} // namespace processor
} // namespace graphflow
