#pragma once

#include "base_evaluator.h"

namespace kuzu {
namespace evaluator {

class LiteralExpressionEvaluator : public BaseExpressionEvaluator {

public:
    LiteralExpressionEvaluator(shared_ptr<Literal> literal)
        : BaseExpressionEvaluator{}, literal{move(literal)} {}

    ~LiteralExpressionEvaluator() = default;

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    inline void evaluate() override {}

    bool select(SelectionVector& selVector) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<LiteralExpressionEvaluator>(literal);
    }

private:
    shared_ptr<Literal> literal;
};

} // namespace evaluator
} // namespace kuzu
