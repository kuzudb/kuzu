#pragma once

#include "expression_evaluator/function_evaluator.h"
#include "expression_evaluator/lambda_evaluator.h"
#include "expression_evaluator/literal_evaluator.h"
#include "expression_evaluator/reference_evaluator.h"
#include "function/function.h"
#include "parser/expression/parsed_expression.h"
#include "processor/expression_mapper.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace function {

struct LambdaFunctionBindData : public FunctionBindData {
    std::shared_ptr<binder::Expression> lambdaExpr;
    std::unique_ptr<evaluator::ExpressionEvaluator> evaluator;

    LambdaFunctionBindData(std::vector<common::LogicalType> paramTypes,
        common::LogicalType dataType, std::shared_ptr<binder::Expression> lambdaExpr)
        : FunctionBindData{std::move(paramTypes), std::move(dataType)},
          lambdaExpr{std::move(lambdaExpr)} {}

    static void setVectorRecur(evaluator::ExpressionEvaluator& evaluator,
        std::shared_ptr<common::ValueVector> resultVec) {
        for (auto& child : evaluator.getChildren()) {
            if (child->isLambdaExpr()) {
                auto& lambdaEvaluator = child->cast<evaluator::LambdaExpressionEvaluator>();
                lambdaEvaluator.setResultVector(resultVec);
                continue;
            }
            setVectorRecur(*child, resultVec);
        }
    }

    void initEvaluator(std::shared_ptr<common::ValueVector> resultVec,
        const processor::ResultSet& resultSet, main::ClientContext* clientContext) {
        setVectorRecur(*evaluator, resultVec);
        evaluator->init(resultSet, clientContext);
        //        auto eval = processor::ExpressionMapper::getEvaluator(lambdaExpr, nullptr);
        //
        //        auto leftEvaluator =
        //        std::make_unique<evaluator::ReferenceExpressionEvaluator>(
        //            processor::DataPos{0, 0}, false);
        //        auto rightEvaluator =
        //            processor::ExpressionMapper::getEvaluator(lambdaExpr->getChild(1),
        //            nullptr);
        //        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> children;
        //        children.push_back(std::move(leftEvaluator));
        //        children.push_back(std::move(rightEvaluator));
        //        evaluator =
        //        std::make_unique<evaluator::FunctionExpressionEvaluator>(lambdaExpr,
        //            std::move(children));

        //        processor::ResultSet resultSet{1};
        //        auto dataChunk = std::make_shared<common::DataChunk>(1);
        //        dataChunk->state->getSelVectorUnsafe().setSelSize(1);
        //        resultSet.insert(0, dataChunk);
        //        dataChunk->insert(0, common::ListVector::getSharedDataVector(&inputVec));
        //        evaluator->init(resultSet, context);
    }

    std::unique_ptr<FunctionBindData> copy() const override {
        auto result = std::make_unique<LambdaFunctionBindData>(copyVector(paramTypes),
            resultType.copy(), lambdaExpr->copy());
        result->evaluator = evaluator->clone();
        return result;
    }
};

} // namespace function
} // namespace kuzu
