#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/processor/include/execution_context.h"
#include "src/processor/include/physical_plan/mapper/mapper_context.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/processor/include/physical_plan/result/result_set_descriptor.h"

using namespace graphflow::binder;
using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class PlanMapper;

class ExpressionMapper {

public:
    unique_ptr<ExpressionEvaluator> mapLogicalExpressionToPhysical(const Expression& expression,
        const MapperContext& mapperContext, ExecutionContext& executionContext);

private:
    unique_ptr<ExpressionEvaluator> mapChildExpressionAndCastToUnstructuredIfNecessary(
        const Expression& expression, bool castToUnstructured, const MapperContext& mapperContext,
        ExecutionContext& executionContext);

    unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToUnstructuredPhysical(
        const Expression& expression, ExecutionContext& executionContext);

    unique_ptr<ExpressionEvaluator> mapLogicalLiteralExpressionToStructuredPhysical(
        const Expression& expression, ExecutionContext& executionContext);

    unique_ptr<ExpressionEvaluator> mapLogicalLeafExpressionToPhysical(
        const Expression& expression, const MapperContext& mapperContext);
};

} // namespace processor
} // namespace graphflow
