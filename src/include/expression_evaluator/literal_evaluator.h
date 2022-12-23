#pragma once

#include "base_evaluator.h"

namespace kuzu {
namespace evaluator {

class LiteralExpressionEvaluator : public BaseExpressionEvaluator {
public:
    LiteralExpressionEvaluator(shared_ptr<Literal> literal)
        : BaseExpressionEvaluator{true /* isResultFlat */}, literal{std::move(literal)} {}

    ~LiteralExpressionEvaluator() = default;

    inline void evaluate() override {}

    bool select(SelectionVector& selVector) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<LiteralExpressionEvaluator>(literal);
    }

protected:
    void resolveResultVector(const ResultSet& resultSet, MemoryManager* memoryManager) override;

private:
    shared_ptr<Literal> literal;
};

} // namespace evaluator
} // namespace kuzu
