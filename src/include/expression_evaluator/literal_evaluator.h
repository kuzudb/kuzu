#pragma once

#include "common/types/value/value.h"
#include "expression_evaluator.h"

namespace kuzu {
namespace evaluator {

class LiteralExpressionEvaluator : public ExpressionEvaluator {
    static constexpr EvaluatorType type_ = EvaluatorType::LITERAL;

public:
    LiteralExpressionEvaluator(std::shared_ptr<binder::Expression> expression, common::Value value)
        : ExpressionEvaluator{type_, std::move(expression), true /* isResultFlat */},
          value{std::move(value)} {}

    void evaluate() override;

    bool select(common::SelectionVector& selVector) override;

    std::unique_ptr<ExpressionEvaluator> clone() override {
        return std::make_unique<LiteralExpressionEvaluator>(expression, value);
    }

protected:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

private:
    common::Value value;
    std::shared_ptr<common::DataChunkState> flatState;
    std::shared_ptr<common::DataChunkState> unflatState;
};

} // namespace evaluator
} // namespace kuzu
