#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalShortestPath : public LogicalOperator {

public:
    LogicalShortestPath(shared_ptr<NodeExpression> sourceNode, shared_ptr<NodeExpression> destNode,
        shared_ptr<RelExpression> rel, shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)},
          sourceNode{std::move(sourceNode)}, destNode{std::move(destNode)}, rel{std::move(rel)} {}

    shared_ptr<NodeExpression> getSrcNodeExpression() { return sourceNode; }

    shared_ptr<NodeExpression> getDestNodeExpression() { return destNode; }

    shared_ptr<RelExpression> getRelExpression() { return rel; }

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SHORTEST_PATH;
    }

    string getExpressionsForPrinting() const override {
        return sourceNode->getRawName() + ("->") + destNode->getRawName();
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalShortestPath>(sourceNode, destNode, rel, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> sourceNode;
    shared_ptr<NodeExpression> destNode;
    shared_ptr<RelExpression> rel;
};

} // namespace planner
} // namespace kuzu