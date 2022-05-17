#pragma once

#include "base_evaluator.h"

namespace graphflow {
namespace evaluator {

class ReferenceExpressionEvaluator : public BaseExpressionEvaluator {

public:
    ReferenceExpressionEvaluator(const DataPos& vectorPos)
        : BaseExpressionEvaluator{}, vectorPos{vectorPos} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    inline void evaluate() override {}

    uint64_t select(sel_t* selectedPos) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<ReferenceExpressionEvaluator>(vectorPos);
    }

private:
    DataPos vectorPos;
};

} // namespace evaluator
} // namespace graphflow
