#pragma once

#include "base_evaluator.h"

namespace kuzu {
namespace evaluator {

class LiteralExpressionEvaluator : public BaseExpressionEvaluator {
public:
    LiteralExpressionEvaluator(std::shared_ptr<common::Value> value)
        : BaseExpressionEvaluator{true /* isResultFlat */}, value{std::move(value)} {}

    ~LiteralExpressionEvaluator() = default;

    inline void evaluate() override {}

    bool select(common::SelectionVector& selVector) override;

    inline std::unique_ptr<BaseExpressionEvaluator> clone() override {
        return make_unique<LiteralExpressionEvaluator>(value);
    }

protected:
    void resolveResultVector(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) override;

private:
    static void copyValueToVector(
        uint8_t* dstValue, common::ValueVector* dstVector, const common::Value* srcValue);

private:
    std::shared_ptr<common::Value> value;
};

} // namespace evaluator
} // namespace kuzu
