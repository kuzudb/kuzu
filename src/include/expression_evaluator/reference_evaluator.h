#pragma once

#include "base_evaluator.h"

namespace kuzu {
namespace evaluator {

class ReferenceExpressionEvaluator : public BaseExpressionEvaluator {
public:
    explicit ReferenceExpressionEvaluator(const DataPos& vectorPos, bool isResultFlat)
        : BaseExpressionEvaluator{isResultFlat}, vectorPos{vectorPos} {}

    inline void evaluate() override {}

    bool select(SelectionVector& selVector) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<ReferenceExpressionEvaluator>(vectorPos, isResultFlat_);
    }

protected:
    inline void resolveResultVector(
        const ResultSet& resultSet, MemoryManager* memoryManager) override {
        resultVector =
            resultSet.dataChunks[vectorPos.dataChunkPos]->valueVectors[vectorPos.valueVectorPos];
    }

private:
    DataPos vectorPos;
};

} // namespace evaluator
} // namespace kuzu
