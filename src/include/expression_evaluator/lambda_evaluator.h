#pragma once

#include "binder/expression/function_expression.h"
#include "expression_evaluator.h"

namespace kuzu {
namespace evaluator {

class LambdaParamEvaluator : public ExpressionEvaluator {
    static constexpr EvaluatorType type_ = EvaluatorType::LAMBDA_PARAM;

public:
    explicit LambdaParamEvaluator(std::shared_ptr<binder::Expression> expression)
        : ExpressionEvaluator{type_, std::move(expression), false /* isResultFlat */} {}

    void evaluate() override {}

    bool select(common::SelectionVector&) override { KU_UNREACHABLE; }

    std::unique_ptr<ExpressionEvaluator> clone() override {
        return std::make_unique<LambdaParamEvaluator>(expression);
    }

protected:
    void resolveResultVector(const processor::ResultSet&, storage::MemoryManager*) override {}
};

struct ListLambdaBindData {
    std::vector<LambdaParamEvaluator*> lambdaParamEvaluators;
    ExpressionEvaluator* rootEvaluator;
};

// E.g. for function list_transform([0,1,2], x->x+1)
// ListLambdaEvaluator has one child that is the evaluator of [0,1,2]
// lambdaRootEvaluator is the evaluator of x+1
// lambdaParamEvaluator is the evaluator of x
class ListLambdaEvaluator : public ExpressionEvaluator {
    static constexpr EvaluatorType type_ = EvaluatorType::LIST_LAMBDA;

public:
    ListLambdaEvaluator(std::shared_ptr<binder::Expression> expression, evaluator_vector_t children)
        : ExpressionEvaluator{type_, expression, std::move(children)} {
        execFunc = expression->constCast<binder::ScalarFunctionExpression>().execFunc;
    }

    void setLambdaRootEvaluator(std::unique_ptr<ExpressionEvaluator> evaluator) {
        lambdaRootEvaluator = std::move(evaluator);
    }

    void init(const processor::ResultSet& resultSet, main::ClientContext* clientContext) override;

    void evaluate() override;

    bool select(common::SelectionVector&) override { KU_UNREACHABLE; }

    std::unique_ptr<ExpressionEvaluator> clone() override {
        auto result = std::make_unique<ListLambdaEvaluator>(expression, cloneVector(children));
        result->setLambdaRootEvaluator(lambdaRootEvaluator->clone());
        return result;
    }

protected:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

private:
    function::scalar_func_exec_t execFunc;
    ListLambdaBindData bindData;

private:
    std::unique_ptr<ExpressionEvaluator> lambdaRootEvaluator;
    std::vector<LambdaParamEvaluator*> lambdaParamEvaluators;
    std::vector<std::shared_ptr<common::ValueVector>> params;
};

} // namespace evaluator
} // namespace kuzu
