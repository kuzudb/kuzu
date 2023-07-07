#pragma once

#include "binder/expression/expression.h"
#include "expression_evaluator/base_evaluator.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "processor/result/result_set_descriptor.h"

namespace kuzu {
namespace processor {

class PlanMapper;

class ExpressionMapper {

public:
    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapExpression(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema);

private:
    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapLiteralExpression(
        const std::shared_ptr<binder::Expression>& expression);

    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapParameterExpression(
        const std::shared_ptr<binder::Expression>& expression);

    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapReferenceExpression(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema);

    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapCaseExpression(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema);

    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapFunctionExpression(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema);

    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapNodeExpression(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema);

    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapRelExpression(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema);

    std::unique_ptr<evaluator::BaseExpressionEvaluator> mapPathExpression(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema& schema);
};

} // namespace processor
} // namespace kuzu
