#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalOrderBy : public LogicalOperator {
public:
    LogicalOrderBy(binder::expression_vector expressionsToOrderBy, std::vector<bool> sortOrders,
        binder::expression_vector expressionsToMaterialize, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ORDER_BY, std::move(child)},
          expressionsToOrderBy{std::move(expressionsToOrderBy)}, isAscOrders{std::move(sortOrders)},
          expressionsToMaterialize{std::move(expressionsToMaterialize)} {}

    void computeSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(expressionsToOrderBy);
    }

    inline binder::expression_vector getExpressionsToOrderBy() const {
        return expressionsToOrderBy;
    }
    inline std::vector<bool> getIsAscOrders() const { return isAscOrders; }
    inline Schema* getSchemaBeforeOrderBy() const { return children[0]->getSchema(); }
    inline binder::expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalOrderBy>(
            expressionsToOrderBy, isAscOrders, expressionsToMaterialize, children[0]->copy());
    }

private:
    binder::expression_vector expressionsToOrderBy;
    std::vector<bool> isAscOrders;
    binder::expression_vector expressionsToMaterialize;
};

} // namespace planner
} // namespace kuzu
