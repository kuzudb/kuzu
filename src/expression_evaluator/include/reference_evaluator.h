#pragma once

#include "base_evaluator.h"

namespace graphflow {
namespace evaluator {

class ReferenceExpressionEvaluator : public BaseExpressionEvaluator {

public:
    ReferenceExpressionEvaluator(const DataPos& vectorPos, bool isVectorFlat)
        : BaseExpressionEvaluator{}, vectorPos{vectorPos}, isVectorFlat{isVectorFlat} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    inline bool isResultVectorFlat() override { return isVectorFlat; }

    inline void evaluate() override {}

    uint64_t select(sel_t* selectedPos) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<ReferenceExpressionEvaluator>(vectorPos, isVectorFlat);
    }

private:
    DataPos vectorPos;
    bool isVectorFlat;
};

} // namespace evaluator
} // namespace graphflow
