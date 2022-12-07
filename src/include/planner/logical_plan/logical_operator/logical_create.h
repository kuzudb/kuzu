#pragma once

#include "logical_create_delete.h"

namespace kuzu {
namespace planner {

class LogicalCreateNode : public LogicalCreateOrDeleteNode {
public:
    LogicalCreateNode(
        vector<pair<shared_ptr<NodeExpression>, shared_ptr<Expression>>> nodeAndPrimaryKeys,
        shared_ptr<LogicalOperator> child)
        : LogicalCreateOrDeleteNode{
              LogicalOperatorType::CREATE_NODE, std::move(nodeAndPrimaryKeys), std::move(child)} {}

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateNode>(nodeAndPrimaryKeys, children[0]->copy());
    }
};

class LogicalCreateRel : public LogicalCreateOrDeleteRel {
public:
    LogicalCreateRel(vector<shared_ptr<RelExpression>> rels,
        vector<vector<expression_pair>> setItemsPerRel, shared_ptr<LogicalOperator> child)
        : LogicalCreateOrDeleteRel{LogicalOperatorType::CREATE_REL, std::move(rels),
              std::move(child)},
          setItemsPerRel{std::move(setItemsPerRel)} {}

    inline vector<expression_pair> getSetItems(uint32_t idx) const { return setItemsPerRel[idx]; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRel>(rels, setItemsPerRel, children[0]->copy());
    }

private:
    vector<vector<expression_pair>> setItemsPerRel;
};

} // namespace planner
} // namespace kuzu
