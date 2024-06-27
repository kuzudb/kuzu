#pragma once

#include "expression_evaluator.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace evaluator {

class LambdaExpressionEvaluator : public ExpressionEvaluator {
public:
    LambdaExpressionEvaluator() : ExpressionEvaluator{false} {}

    void evaluate() override {}

    bool select(common::SelectionVector& /*selVector*/) override { return true; }

    void setResultVector(std::shared_ptr<common::ValueVector> vector) { resultVector = vector; }

    bool isLambdaExpr() const override { return true; }

    std::unique_ptr<ExpressionEvaluator> clone() override {
        return std::make_unique<LambdaExpressionEvaluator>();
    }

protected:
    void resolveResultVector(const processor::ResultSet& /*resultSet*/,
        storage::MemoryManager* /*memoryManager*/) override {}
};

} // namespace evaluator
} // namespace kuzu
