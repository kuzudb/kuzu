#pragma once

#include "base_logical_operator.h"
#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {

class LogicalUpdateNode : public LogicalOperator {
public:
    LogicalUpdateNode(LogicalOperatorType operatorType, vector<shared_ptr<NodeExpression>> nodes,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType, std::move(child)}, nodes{std::move(nodes)} {}
    ~LogicalUpdateNode() override = default;

    inline void computeSchema() override { copyChildSchema(0); }

    inline string getExpressionsForPrinting() const override {
        expression_vector expressions;
        for (auto& node : nodes) {
            expressions.push_back(node);
        }
        return ExpressionUtil::toString(expressions);
    }

    inline size_t getNumNodes() const { return nodes.size(); }
    inline shared_ptr<NodeExpression> getNode(uint32_t idx) const { return nodes[idx]; }

    unique_ptr<LogicalOperator> copy() override = 0;

protected:
    vector<shared_ptr<NodeExpression>> nodes;
};

class LogicalUpdateRel : public LogicalOperator {
public:
    LogicalUpdateRel(LogicalOperatorType operatorType, vector<shared_ptr<RelExpression>> rels,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType, std::move(child)}, rels{std::move(rels)} {}
    ~LogicalUpdateRel() override = default;

    inline void computeSchema() override { copyChildSchema(0); }

    inline string getExpressionsForPrinting() const override {
        expression_vector expressions;
        for (auto& rel : rels) {
            expressions.push_back(rel);
        }
        return ExpressionUtil::toString(expressions);
    }

    inline uint32_t getNumRels() const { return rels.size(); }
    inline shared_ptr<RelExpression> getRel(uint32_t idx) const { return rels[idx]; }

    unique_ptr<LogicalOperator> copy() override = 0;

protected:
    vector<shared_ptr<RelExpression>> rels;
};

} // namespace planner
} // namespace kuzu
