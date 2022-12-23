#pragma once

#include "logical_update.h"

namespace kuzu {
namespace planner {

class LogicalCreateNode : public LogicalUpdateNode {
public:
    LogicalCreateNode(vector<shared_ptr<NodeExpression>> nodes, expression_vector primaryKeys,
        shared_ptr<LogicalOperator> child)
        : LogicalUpdateNode{LogicalOperatorType::CREATE_NODE, std::move(nodes), std::move(child)},
          primaryKeys{std::move(primaryKeys)} {}
    ~LogicalCreateNode() override = default;

    void computeSchema() override;

    inline shared_ptr<Expression> getPrimaryKey(size_t idx) const { return primaryKeys[idx]; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateNode>(nodes, primaryKeys, children[0]->copy());
    }

private:
    expression_vector primaryKeys;
};

class LogicalCreateRel : public LogicalUpdateRel {
public:
    LogicalCreateRel(vector<shared_ptr<RelExpression>> rels,
        vector<vector<expression_pair>> setItemsPerRel, shared_ptr<LogicalOperator> child)
        : LogicalUpdateRel{LogicalOperatorType::CREATE_REL, std::move(rels), std::move(child)},
          setItemsPerRel{std::move(setItemsPerRel)} {}
    ~LogicalCreateRel() override = default;

    inline vector<expression_pair> getSetItems(uint32_t idx) const { return setItemsPerRel[idx]; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRel>(rels, setItemsPerRel, children[0]->copy());
    }

private:
    vector<vector<expression_pair>> setItemsPerRel;
};

} // namespace planner
} // namespace kuzu
