#pragma once

#include "logical_update.h"
#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

class LogicalDeleteNode : public LogicalUpdateNode {
public:
    LogicalDeleteNode(std::vector<std::shared_ptr<binder::NodeExpression>> nodes,
        binder::expression_vector primaryKeys, std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateNode{LogicalOperatorType::DELETE_NODE, std::move(nodes), std::move(child)},
          primaryKeys{std::move(primaryKeys)} {}
    ~LogicalDeleteNode() override = default;

    inline std::shared_ptr<binder::Expression> getPrimaryKey(size_t idx) const {
        return primaryKeys[idx];
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDeleteNode>(nodes, primaryKeys, children[0]->copy());
    }

private:
    binder::expression_vector primaryKeys;
};

class LogicalDeleteRel : public LogicalUpdateRel {
public:
    LogicalDeleteRel(std::vector<std::shared_ptr<binder::RelExpression>> rels,
        std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateRel{LogicalOperatorType::DELETE_REL, std::move(rels), std::move(child)} {}
    ~LogicalDeleteRel() override = default;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDeleteRel>(rels, children[0]->copy());
    }
};

class LogicalDeleteRelFactorizationSolver {
public:
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(
        const std::shared_ptr<binder::RelExpression>& rel, LogicalOperator* deleteRelChild) {
        std::unordered_set<f_group_pos> result;
        auto schema = deleteRelChild->getSchema();
        result.insert(schema->getGroupPos(*rel->getSrcNode()->getInternalIDProperty()));
        result.insert(schema->getGroupPos(*rel->getDstNode()->getInternalIDProperty()));
        return FlattenAllFactorizationSolver::getGroupsPosToFlatten(result, schema);
    }
};

} // namespace planner
} // namespace kuzu
