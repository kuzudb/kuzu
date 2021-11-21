#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/execution_context.h"
#include "src/processor/include/physical_plan/mapper/physical_operator_info.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace graphflow::binder;
using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class PlanMapper;

class ExpressionMapper {

public:
    explicit ExpressionMapper(PlanMapper* planMapper) : planMapper{planMapper} {};

    unique_ptr<ExpressionEvaluator> mapToPhysical(const Expression& expression,
        const PhysicalOperatorsInfo& physicalOperatorInfo, ExecutionContext& context);

private:
    unique_ptr<ExpressionEvaluator> mapChildExpressionAndCastToUnstructuredIfNecessary(
        const Expression& expression, bool castToUnstructured,
        const PhysicalOperatorsInfo& physicalOperatorInfo, ExecutionContext& context);

    unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToUnstructuredPhysical(
        const Expression& expression, ExecutionContext& context);

    unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToStructuredPhysical(
        const Expression& expression, ExecutionContext& context);

    unique_ptr<ExpressionEvaluator> mapLogicalLeafExpressionToPhysical(
        const Expression& expression, const PhysicalOperatorsInfo& physicalOperatorInfo);

    unique_ptr<ExpressionEvaluator> mapLogicalExistentialSubqueryExpressionToPhysical(
        const Expression& expression, const PhysicalOperatorsInfo& physicalOperatorInfo,
        ExecutionContext& context);

private:
    PlanMapper* planMapper;
};

} // namespace processor
} // namespace graphflow
