#pragma once

#include "logical_update.h"
#include "planner/logical_plan/logical_operator/flatten_resolver.h"

namespace kuzu {
namespace planner {

class LogicalDeleteNode : public LogicalUpdateNode {
public:
    LogicalDeleteNode(std::vector<std::shared_ptr<binder::NodeExpression>> nodes,
        binder::expression_vector primaryKeys, std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateNode{LogicalOperatorType::DELETE_NODE, std::move(nodes), std::move(child)},
          primaryKeys{std::move(primaryKeys)} {}
    ~LogicalDeleteNode() override = default;

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

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

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline f_group_pos_set getGroupsPosToFlatten(uint32_t relIdx) {
        f_group_pos_set result;
        auto rel = rels[relIdx];
        auto childSchema = children[0]->getSchema();
        result.insert(childSchema->getGroupPos(*rel->getSrcNode()->getInternalIDProperty()));
        result.insert(childSchema->getGroupPos(*rel->getDstNode()->getInternalIDProperty()));
        return factorization::FlattenAll::getGroupsPosToFlatten(result, childSchema);
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDeleteRel>(rels, children[0]->copy());
    }
};

} // namespace planner
} // namespace kuzu
