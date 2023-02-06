#pragma once

#include "base_evaluator.h"

namespace kuzu {
namespace evaluator {

class ReferenceExpressionEvaluator : public BaseExpressionEvaluator {
public:
    explicit ReferenceExpressionEvaluator(const processor::DataPos& vectorPos, bool isResultFlat)
        : BaseExpressionEvaluator{isResultFlat}, vectorPos{vectorPos} {}

    inline void evaluate() override {}

    bool select(common::SelectionVector& selVector) override;

    inline std::unique_ptr<BaseExpressionEvaluator> clone() override {
        return std::make_unique<ReferenceExpressionEvaluator>(vectorPos, isResultFlat_);
    }

protected:
    inline void resolveResultVector(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) override {
        resultVector =
            resultSet.dataChunks[vectorPos.dataChunkPos]->valueVectors[vectorPos.valueVectorPos];
    }

private:
    processor::DataPos vectorPos;
};

} // namespace evaluator
} // namespace kuzu
