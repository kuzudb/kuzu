#pragma once

#include "base_logical_operator.h"
#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {

class LogicalCreateOrDeleteNode : public LogicalOperator {
public:
    LogicalCreateOrDeleteNode(
        vector<pair<shared_ptr<NodeExpression>, shared_ptr<Expression>>> nodeAndPrimaryKeys,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, nodeAndPrimaryKeys{std::move(nodeAndPrimaryKeys)} {}

    LogicalOperatorType getLogicalOperatorType() const override = 0;

    inline string getExpressionsForPrinting() const override {
        expression_vector expressions;
        for (auto& [node, primaryKey] : nodeAndPrimaryKeys) {
            expressions.push_back(node);
        }
        return ExpressionUtil::toString(expressions);
    }

    inline vector<pair<shared_ptr<NodeExpression>, shared_ptr<Expression>>>
    getNodeAndPrimaryKeys() const {
        return nodeAndPrimaryKeys;
    }

    unique_ptr<LogicalOperator> copy() override = 0;

protected:
    vector<pair<shared_ptr<NodeExpression>, shared_ptr<Expression>>> nodeAndPrimaryKeys;
};

class LogicalCreateOrDeleteRel : public LogicalOperator {
public:
    LogicalCreateOrDeleteRel(
        vector<shared_ptr<RelExpression>> rels, shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, rels{std::move(rels)} {}

    LogicalOperatorType getLogicalOperatorType() const override = 0;

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
