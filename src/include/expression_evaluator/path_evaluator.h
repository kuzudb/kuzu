#pragma once

#include "base_evaluator.h"
#include "binder/expression/path_expression.h"

namespace kuzu {
namespace evaluator {

class PathExpressionEvaluator : public ExpressionEvaluator {
public:
    PathExpressionEvaluator(std::shared_ptr<binder::PathExpression> pathExpression,
        std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : ExpressionEvaluator{std::move(children)}, pathExpression{std::move(pathExpression)} {}

    void init(const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) final;

    void evaluate() final;

    bool select(common::SelectionVector& selVector) final {
        throw common::NotImplementedException("PathExpressionEvaluator::select");
    }

    inline std::unique_ptr<ExpressionEvaluator> clone() final {
        std::vector<std::unique_ptr<ExpressionEvaluator>> clonedChildren;
        for (auto& child : children) {
            clonedChildren.push_back(child->clone());
        }
        return make_unique<PathExpressionEvaluator>(pathExpression, std::move(clonedChildren));
    }

private:
    struct InputVectors {
        // input can either be NODE, REL or RECURSIVE_REL
        common::ValueVector* input = nullptr;
        // nodesInput is LIST[NODE] for RECURSIVE_REL input and nullptr otherwise
        common::ValueVector* nodesInput = nullptr;
        // nodesDataInput is NODE for RECURSIVE_REL and nullptr otherwise
        common::ValueVector* nodesDataInput = nullptr;
        // relsInput is LIST[REL] for RECURSIVE_REL input and nullptr otherwise
        common::ValueVector* relsInput = nullptr;
        // relsDataInput is REL for RECURSIVE_REL input and nullptr otherwise
        common::ValueVector* relsDataInput = nullptr;

        std::vector<common::ValueVector*> nodeFieldVectors;
        std::vector<common::ValueVector*> relFieldVectors;
    };

    void resolveResultVector(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) final;

    void copyNodes(common::sel_t resultPos);
    void copyRels(common::sel_t resultPos);

    void copyFieldVectors(common::offset_t inputVectorPos,
        const std::vector<common::ValueVector*>& inputFieldVectors,
        common::offset_t& resultVectorPos,
        const std::vector<common::ValueVector*>& resultFieldVectors);

private:
    std::shared_ptr<binder::PathExpression> pathExpression;
    std::vector<std::unique_ptr<InputVectors>> inputVectorsPerChild;
    common::ValueVector* resultNodesVector; // LIST[NODE]
    common::ValueVector* resultRelsVector;  // LIST[REL]
    std::vector<common::ValueVector*> resultNodesFieldVectors;
    std::vector<common::ValueVector*> resultRelsFieldVectors;
};

} // namespace evaluator
} // namespace kuzu
