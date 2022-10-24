#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"
#include "src/binder/expression/include/rel_expression.h"

namespace graphflow {
namespace planner {

class LogicalCreateNode : public LogicalOperator {
public:
    LogicalCreateNode(vector<shared_ptr<NodeExpression>> nodes, shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, nodes{std::move(nodes)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_CREATE_NODE;
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
        return make_unique<LogicalCreateNode>(nodes, children[0]->copy());
    }

private:
    vector<shared_ptr<NodeExpression>> nodes;
};

class LogicalCreateRel : public LogicalOperator {
public:
    LogicalCreateRel(vector<shared_ptr<RelExpression>> rels,
        vector<vector<expression_pair>> setItemsPerRel, shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, rels{std::move(rels)}, setItemsPerRel{std::move(
                                                                        setItemsPerRel)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_CREATE_REL;
    }

    inline string getExpressionsForPrinting() const override {
        expression_vector expressions;
        for (auto& rel : rels) {
            expressions.push_back(rel);
        }
        return ExpressionUtil::toString(expressions);
    }

    inline uint32_t getNumRels() const { return rels.size(); }
    inline shared_ptr<RelExpression> getRel(uint32_t idx) const { return rels[idx]; }
    inline vector<expression_pair> getSetItems(uint32_t idx) const { return setItemsPerRel[idx]; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateRel>(rels, setItemsPerRel, children[0]->copy());
    }

private:
    vector<shared_ptr<RelExpression>> rels;
    vector<vector<expression_pair>> setItemsPerRel;
};

} // namespace planner
} // namespace graphflow
