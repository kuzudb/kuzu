#pragma once

#include "logical_create_delete.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalDeleteNode : public LogicalCreateOrDeleteNode {
public:
    LogicalDeleteNode(
        vector<pair<shared_ptr<NodeExpression>, shared_ptr<Expression>>> nodeAndPrimaryKeys,
        shared_ptr<LogicalOperator> child)
        : LogicalCreateOrDeleteNode{
              LogicalOperatorType::DELETE_NODE, std::move(nodeAndPrimaryKeys), std::move(child)} {}

    inline void computeSchema() override { copyChildSchema(0); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDeleteNode>(nodeAndPrimaryKeys, children[0]->copy());
    }
};

class LogicalDeleteRel : public LogicalCreateOrDeleteRel {
public:
    LogicalDeleteRel(vector<shared_ptr<RelExpression>> rels, shared_ptr<LogicalOperator> child)
        : LogicalCreateOrDeleteRel{
              LogicalOperatorType::DELETE_REL, std::move(rels), std::move(child)} {}

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDeleteRel>(rels, children[0]->copy());
    }
};

} // namespace planner
} // namespace kuzu
