#pragma once

#include "logical_update.h"

namespace kuzu {
namespace planner {

class LogicalCreateNode : public LogicalUpdateNode {
public:
    LogicalCreateNode(std::vector<std::shared_ptr<binder::NodeExpression>> nodes,
        binder::expression_vector primaryKeys, std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateNode{LogicalOperatorType::CREATE_NODE, std::move(nodes), std::move(child)},
          primaryKeys{std::move(primaryKeys)} {}
    ~LogicalCreateNode() override = default;

    void computeSchema() override;

    inline std::shared_ptr<binder::Expression> getPrimaryKey(size_t idx) const {
        return primaryKeys[idx];
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateNode>(nodes, primaryKeys, children[0]->copy());
    }

private:
    binder::expression_vector primaryKeys;
};

class LogicalCreateRel : public LogicalUpdateRel {
public:
    LogicalCreateRel(std::vector<std::shared_ptr<binder::RelExpression>> rels,
        std::vector<std::vector<binder::expression_pair>> setItemsPerRel,
        std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateRel{LogicalOperatorType::CREATE_REL, std::move(rels), std::move(child)},
          setItemsPerRel{std::move(setItemsPerRel)} {}
    ~LogicalCreateRel() override = default;

    inline std::vector<binder::expression_pair> getSetItems(uint32_t idx) const {
        return setItemsPerRel[idx];
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRel>(rels, setItemsPerRel, children[0]->copy());
    }

private:
    std::vector<std::vector<binder::expression_pair>> setItemsPerRel;
};

} // namespace planner
} // namespace kuzu
