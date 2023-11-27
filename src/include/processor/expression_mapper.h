#pragma once

#include "binder/expression/expression.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/result/result_set_descriptor.h"

namespace kuzu {
namespace processor {

class ExpressionMapper {
public:
    static std::unique_ptr<evaluator::ExpressionEvaluator> getEvaluator(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema* schema);
    static std::unique_ptr<evaluator::ExpressionEvaluator> getConstantEvaluator(
        const std::shared_ptr<binder::Expression>& expression);

private:
    static std::unique_ptr<evaluator::ExpressionEvaluator> getLiteralEvaluator(
        const binder::Expression& expression);

    static std::unique_ptr<evaluator::ExpressionEvaluator> getParameterEvaluator(
        const binder::Expression& expression);

    static std::unique_ptr<evaluator::ExpressionEvaluator> getReferenceEvaluator(
        const std::shared_ptr<binder::Expression>& expression, const planner::Schema* schema);

    static std::unique_ptr<evaluator::ExpressionEvaluator> getCaseEvaluator(
        std::shared_ptr<binder::Expression> expression, const planner::Schema* schema);

    static std::unique_ptr<evaluator::ExpressionEvaluator> getFunctionEvaluator(
        std::shared_ptr<binder::Expression> expression, const planner::Schema* schema);

    static std::unique_ptr<evaluator::ExpressionEvaluator> getNodeEvaluator(
        std::shared_ptr<binder::Expression> expression, const planner::Schema* schema);

    static std::unique_ptr<evaluator::ExpressionEvaluator> getRelEvaluator(
        std::shared_ptr<binder::Expression> expression, const planner::Schema* schema);

    static std::unique_ptr<evaluator::ExpressionEvaluator> getPathEvaluator(
        std::shared_ptr<binder::Expression> expression, const planner::Schema* schema);

    static std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> getEvaluators(
        const binder::expression_vector& expressions, const planner::Schema* schema);
};

} // namespace processor
} // namespace kuzu
