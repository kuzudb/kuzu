#pragma once

#include "common/types/value/value.h"
#include "expression_evaluator.h"

namespace kuzu {
namespace evaluator {

class LiteralExpressionEvaluator : public ExpressionEvaluator {
public:
    explicit LiteralExpressionEvaluator(common::Value value)
        : ExpressionEvaluator{true /* isResultFlat */}, value{std::move(value)} {}

    void evaluate(main::ClientContext* /* clientContext */) override {}

    bool select(common::SelectionVector& selVector, main::ClientContext* clientContext) override;

    std::unique_ptr<ExpressionEvaluator> clone() override {
        return std::make_unique<LiteralExpressionEvaluator>(value);
    }

protected:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

private:
    common::Value value;
};

} // namespace evaluator
} // namespace kuzu
