#pragma once

#include "base_evaluator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace evaluator {

class NodeRelExpressionEvaluator : public ExpressionEvaluator {
public:
    NodeRelExpressionEvaluator(std::shared_ptr<binder::Expression> nodeOrRel,
        std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : ExpressionEvaluator{std::move(children)}, nodeOrRel{std::move(nodeOrRel)} {}

    void evaluate() final;

    bool select(common::SelectionVector& selVector) final {
        throw common::NotImplementedException("NodeExpressionEvaluator::select");
    }

    inline std::unique_ptr<ExpressionEvaluator> clone() final {
        std::vector<std::unique_ptr<ExpressionEvaluator>> clonedChildren;
        for (auto& child : children) {
            clonedChildren.push_back(child->clone());
        }
        return make_unique<NodeRelExpressionEvaluator>(nodeOrRel, std::move(clonedChildren));
    }

private:
    void resolveResultVector(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) override;

private:
    std::shared_ptr<binder::Expression> nodeOrRel;
    std::vector<std::shared_ptr<common::ValueVector>> parameters;
};

} // namespace evaluator
} // namespace kuzu
