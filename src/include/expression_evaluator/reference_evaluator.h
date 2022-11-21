#pragma once

#include "base_evaluator.h"

namespace kuzu {
namespace evaluator {

class ReferenceExpressionEvaluator : public BaseExpressionEvaluator {

public:
    explicit ReferenceExpressionEvaluator(const DataPos& vectorPos)
        : BaseExpressionEvaluator{}, vectorPos{vectorPos} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    inline void evaluate() override {}

    bool select(SelectionVector& selVector) override;

    inline unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<ReferenceExpressionEvaluator>(vectorPos);
    }

private:
    DataPos vectorPos;
};

} // namespace evaluator
} // namespace kuzu
