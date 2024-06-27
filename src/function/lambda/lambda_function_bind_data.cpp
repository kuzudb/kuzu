#include "function/lambda/lambda_function_bind_data.h"

namespace kuzu {
namespace function {

void LambdaFunctionBindData::setLambdaVarVector(evaluator::ExpressionEvaluator& evaluator,
    std::shared_ptr<common::ValueVector> varVector) {
    for (auto& child : evaluator.getChildren()) {
        if (child->isLambdaExpr()) {
            auto& lambdaEvaluator = child->cast<evaluator::LambdaExpressionEvaluator>();
            lambdaEvaluator.setResultVector(varVector);
            continue;
        }
        setLambdaVarVector(*child, varVector);
    }
}

void LambdaFunctionBindData::initEvaluator(std::shared_ptr<common::ValueVector> resultVec,
    const processor::ResultSet& resultSet, main::ClientContext* clientContext) const {
    setLambdaVarVector(*evaluator, resultVec);
    evaluator->init(resultSet, clientContext);
}

std::unique_ptr<FunctionBindData> LambdaFunctionBindData::copy() const {
    auto result = std::make_unique<LambdaFunctionBindData>(copyVector(paramTypes),
        resultType.copy(), lambdaExpr->copy());
    result->evaluator = evaluator->clone();
    return result;
}

} // namespace function
} // namespace kuzu
