#pragma once

#include "logical_update.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalDeleteNode : public LogicalUpdateNode {
public:
    LogicalDeleteNode(vector<shared_ptr<NodeExpression>> nodes, expression_vector primaryKeys,
        shared_ptr<LogicalOperator> child)
        : LogicalUpdateNode{LogicalOperatorType::DELETE_NODE, std::move(nodes), std::move(child)},
          primaryKeys{std::move(primaryKeys)} {}
    ~LogicalDeleteNode() override = default;

    inline shared_ptr<Expression> getPrimaryKey(size_t idx) const { return primaryKeys[idx]; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDeleteNode>(nodes, primaryKeys, children[0]->copy());
    }

private:
    expression_vector primaryKeys;
};

class LogicalDeleteRel : public LogicalUpdateRel {
public:
    LogicalDeleteRel(vector<shared_ptr<RelExpression>> rels, shared_ptr<LogicalOperator> child)
        : LogicalUpdateRel{LogicalOperatorType::DELETE_REL, std::move(rels), std::move(child)} {}
    ~LogicalDeleteRel() override = default;

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDeleteRel>(rels, children[0]->copy());
    }
};

} // namespace planner
} // namespace kuzu
