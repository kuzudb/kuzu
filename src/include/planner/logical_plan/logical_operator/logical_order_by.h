#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalOrderBy : public LogicalOperator {
public:
    LogicalOrderBy(expression_vector expressionsToOrderBy, vector<bool> sortOrders,
        expression_vector expressionsToMaterialize, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ORDER_BY, std::move(child)},
          expressionsToOrderBy{std::move(expressionsToOrderBy)}, isAscOrders{std::move(sortOrders)},
          expressionsToMaterialize{std::move(expressionsToMaterialize)} {}

    void computeSchema() override;

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToOrderBy() const { return expressionsToOrderBy; }
    inline vector<bool> getIsAscOrders() const { return isAscOrders; }
    inline Schema* getSchemaBeforeOrderBy() const { return children[0]->getSchema(); }
    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalOrderBy>(
            expressionsToOrderBy, isAscOrders, expressionsToMaterialize, children[0]->copy());
    }

private:
    expression_vector expressionsToOrderBy;
    vector<bool> isAscOrders;
    expression_vector expressionsToMaterialize;
};

} // namespace planner
} // namespace kuzu
