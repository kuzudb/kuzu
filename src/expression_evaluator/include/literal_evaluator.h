#pragma once

#include "base_evaluator.h"

namespace graphflow {
namespace evaluator {

class LiteralExpressionEvaluator : public BaseExpressionEvaluator {

public:
    LiteralExpressionEvaluator(const Literal& literal, bool castToUnstructured)
        : BaseExpressionEvaluator{}, literal{literal}, castToUnstructured{castToUnstructured} {}

    ~LiteralExpressionEvaluator() = default;

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    inline bool isResultVectorFlat() override { return true; }

    inline void evaluate() override {}

    uint64_t select(sel_t* selectedPos) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<LiteralExpressionEvaluator>(literal, castToUnstructured);
    }

private:
    Literal literal;
    // This is a performance optimization. If we know a literal will be casted to unstructured, we
    // can perform casting within literal expression evaluator and avoid an extra layer of evaluator
    // that performs casting.
    bool castToUnstructured;
};

} // namespace evaluator
} // namespace graphflow
