#pragma once

#include "base_logical_operator.h"
#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {

class LogicalUpdateNode : public LogicalOperator {
public:
    LogicalUpdateNode(LogicalOperatorType operatorType,
        std::vector<std::shared_ptr<binder::NodeExpression>> nodes,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType, std::move(child)}, nodes{std::move(nodes)} {}
    ~LogicalUpdateNode() override = default;

    inline std::string getExpressionsForPrinting() const override {
        binder::expression_vector expressions;
        for (auto& node : nodes) {
            expressions.push_back(node);
        }
        return binder::ExpressionUtil::toString(expressions);
    }

    inline size_t getNumNodes() const { return nodes.size(); }
    inline std::shared_ptr<binder::NodeExpression> getNode(uint32_t idx) const {
        return nodes[idx];
    }

    std::unique_ptr<LogicalOperator> copy() override = 0;

protected:
    std::vector<std::shared_ptr<binder::NodeExpression>> nodes;
};

class LogicalUpdateRel : public LogicalOperator {
public:
    LogicalUpdateRel(LogicalOperatorType operatorType,
        std::vector<std::shared_ptr<binder::RelExpression>> rels,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType, std::move(child)}, rels{std::move(rels)} {}
    ~LogicalUpdateRel() override = default;

    inline std::string getExpressionsForPrinting() const override {
        binder::expression_vector expressions;
        for (auto& rel : rels) {
            expressions.push_back(rel);
        }
        return binder::ExpressionUtil::toString(expressions);
    }

    inline uint32_t getNumRels() const { return rels.size(); }
    inline std::shared_ptr<binder::RelExpression> getRel(uint32_t idx) const { return rels[idx]; }

    std::unique_ptr<LogicalOperator> copy() override = 0;

protected:
    std::vector<std::shared_ptr<binder::RelExpression>> rels;
};

} // namespace planner
} // namespace kuzu
