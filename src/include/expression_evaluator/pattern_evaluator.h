#pragma once

#include "binder/expression/expression.h"
#include "expression_evaluator.h"

namespace kuzu {
namespace evaluator {

class PatternExpressionEvaluator : public ExpressionEvaluator {
public:
    PatternExpressionEvaluator(std::shared_ptr<binder::Expression> pattern,
        std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : ExpressionEvaluator{std::move(children)}, pattern{std::move(pattern)} {}

    void evaluate() override;

    bool select(common::SelectionVector&) override { KU_UNREACHABLE; }

    std::unique_ptr<ExpressionEvaluator> clone() override;

protected:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

    virtual void initFurther(const processor::ResultSet& resultSet);

protected:
    std::shared_ptr<binder::Expression> pattern;
    common::ValueVector* idVector;
    std::vector<std::shared_ptr<common::ValueVector>> parameters;
};

class UndirectedRelExpressionEvaluator final : public PatternExpressionEvaluator {
public:
    UndirectedRelExpressionEvaluator(std::shared_ptr<binder::Expression> pattern,
        std::vector<std::unique_ptr<ExpressionEvaluator>> children,
        std::unique_ptr<ExpressionEvaluator> directionEvaluator)
        : PatternExpressionEvaluator{std::move(pattern), std::move(children)},
          directionEvaluator{std::move(directionEvaluator)} {}

    void evaluate() override;

    void initFurther(const processor::ResultSet& resultSet) override;

    std::unique_ptr<ExpressionEvaluator> clone() override;

private:
    common::ValueVector* srcIDVector;
    common::ValueVector* dstIDVector;
    common::ValueVector* directionVector;
    std::unique_ptr<ExpressionEvaluator> directionEvaluator;
};

} // namespace evaluator
} // namespace kuzu
