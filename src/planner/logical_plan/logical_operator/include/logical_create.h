#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"
#include "src/binder/expression/include/rel_expression.h"

namespace graphflow {
namespace planner {

struct NodeAndPrimaryKey {
    shared_ptr<NodeExpression> node;
    shared_ptr<Expression> primaryKey;

    NodeAndPrimaryKey(shared_ptr<NodeExpression> node, shared_ptr<Expression> primaryKey)
        : node{std::move(node)}, primaryKey{std::move(primaryKey)} {}

    inline unique_ptr<NodeAndPrimaryKey> copy() const {
        return make_unique<NodeAndPrimaryKey>(node, primaryKey);
    }
};

class LogicalCreateNode : public LogicalOperator {
public:
    LogicalCreateNode(
        vector<unique_ptr<NodeAndPrimaryKey>> nodeAndPrimaryKeys, shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, nodeAndPrimaryKeys{std::move(nodeAndPrimaryKeys)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_CREATE_NODE;
    }

    inline string getExpressionsForPrinting() const override {
        expression_vector expressions;
        for (auto& nodeAndPrimaryKey : nodeAndPrimaryKeys) {
            expressions.push_back(nodeAndPrimaryKey->node);
        }
        return ExpressionUtil::toString(expressions);
    }

    inline uint32_t getNumNodeAndPrimaryKeys() const { return nodeAndPrimaryKeys.size(); }
    inline NodeAndPrimaryKey* getNodeAndPrimaryKey(uint32_t idx) {
        return nodeAndPrimaryKeys[idx].get();
    }

    inline unique_ptr<LogicalOperator> copy() override {
        vector<unique_ptr<NodeAndPrimaryKey>> copiedNodeAndPrimaryKeys;
        for (auto& nodeAndPrimaryKey : nodeAndPrimaryKeys) {
            copiedNodeAndPrimaryKeys.push_back(nodeAndPrimaryKey->copy());
        }
        return make_unique<LogicalCreateNode>(
            std::move(copiedNodeAndPrimaryKeys), children[0]->copy());
    }

private:
    vector<unique_ptr<NodeAndPrimaryKey>> nodeAndPrimaryKeys;
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
