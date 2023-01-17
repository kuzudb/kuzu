#pragma once

#include "base_evaluator.h"

namespace kuzu {
namespace evaluator {

class LiteralExpressionEvaluator : public BaseExpressionEvaluator {
public:
    LiteralExpressionEvaluator(shared_ptr<Value> value)
        : BaseExpressionEvaluator{true /* isResultFlat */}, value{std::move(value)} {}

    ~LiteralExpressionEvaluator() = default;

    inline void evaluate() override {}

    bool select(SelectionVector& selVector) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<LiteralExpressionEvaluator>(value);
    }

protected:
    void resolveResultVector(const ResultSet& resultSet, MemoryManager* memoryManager) override;

private:
    shared_ptr<Value> value;
};

} // namespace evaluator
} // namespace kuzu
