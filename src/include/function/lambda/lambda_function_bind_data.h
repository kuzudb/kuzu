#pragma once

#include "expression_evaluator/function_evaluator.h"
#include "expression_evaluator/lambda_evaluator.h"
#include "expression_evaluator/reference_evaluator.h"
#include "function/function.h"
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

    static void setLambdaVarVector(evaluator::ExpressionEvaluator& evaluator,
        std::shared_ptr<common::ValueVector> varVector);

    void initEvaluator(std::shared_ptr<common::ValueVector> resultVec,
        const processor::ResultSet& resultSet, main::ClientContext* clientContext) const;

    std::unique_ptr<FunctionBindData> copy() const override;
};

} // namespace function
} // namespace kuzu
