#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace planner {

class LogicalCreate : public LogicalOperator {
public:
    LogicalCreate(vector<shared_ptr<NodeExpression>> nodes, shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, nodes{std::move(nodes)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_CREATE;
    }

    inline string getExpressionsForPrinting() const override {
        expression_vector expressions;
        for (auto& node : nodes) {
            expressions.push_back(node);
        }
        return ExpressionUtil::toString(expressions);
    }

    inline vector<shared_ptr<NodeExpression>> getNodes() const { return nodes; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreate>(nodes, children[0]->copy());
    }

private:
    vector<shared_ptr<NodeExpression>> nodes;
};

} // namespace planner
} // namespace graphflow
