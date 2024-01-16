#pragma once

#include "expression_evaluator.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace evaluator {

class LiteralExpressionEvaluator : public ExpressionEvaluator {
public:
    explicit LiteralExpressionEvaluator(std::shared_ptr<common::Value> value)
        : ExpressionEvaluator{true /* isResultFlat */}, value{std::move(value)} {}

    ~LiteralExpressionEvaluator() override = default;

    inline void evaluate(main::ClientContext* /* clientContext */) override {}

    bool select(common::SelectionVector& selVector, main::ClientContext* clientContext) override;

    inline std::unique_ptr<ExpressionEvaluator> clone() override {
        return make_unique<LiteralExpressionEvaluator>(value);
    }

protected:
    void resolveResultVector(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) override;

private:
    std::shared_ptr<common::Value> value;
};

} // namespace evaluator
} // namespace kuzu
