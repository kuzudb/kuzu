#pragma once

#include "binder/expression/expression.h"
#include "expression_evaluator.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace evaluator {

class NodeRelExpressionEvaluator final : public ExpressionEvaluator {
public:
    NodeRelExpressionEvaluator(std::shared_ptr<binder::Expression> nodeOrRel,
        std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : ExpressionEvaluator{std::move(children)}, nodeOrRel{std::move(nodeOrRel)} {}

    void evaluate(main::ClientContext* clientContext) override;

    bool select(common::SelectionVector& /*selVector*/,
        main::ClientContext* /*clientContext*/) override {
        KU_UNREACHABLE;
    }

    inline std::unique_ptr<ExpressionEvaluator> clone() override {
        std::vector<std::unique_ptr<ExpressionEvaluator>> clonedChildren;
        clonedChildren.reserve(children.size());
        for (auto& child : children) {
            clonedChildren.push_back(child->clone());
        }
        return make_unique<NodeRelExpressionEvaluator>(nodeOrRel, std::move(clonedChildren));
    }

private:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

private:
    std::shared_ptr<binder::Expression> nodeOrRel;
    std::vector<std::shared_ptr<common::ValueVector>> parameters;
};

} // namespace evaluator
} // namespace kuzu
