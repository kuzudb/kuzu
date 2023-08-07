#pragma once

#include "logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalOrderBy : public LogicalOperator {
public:
    LogicalOrderBy(binder::expression_vector expressionsToOrderBy, std::vector<bool> sortOrders,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ORDER_BY, std::move(child)},
          expressionsToOrderBy{std::move(expressionsToOrderBy)}, isAscOrders{
                                                                     std::move(sortOrders)} {}

    f_group_pos_set getGroupsPosToFlatten();

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(expressionsToOrderBy);
    }

    inline binder::expression_vector getExpressionsToOrderBy() const {
        return expressionsToOrderBy;
    }
    inline std::vector<bool> getIsAscOrders() const { return isAscOrders; }
    inline binder::expression_vector getExpressionsToMaterialize() const {
        return children[0]->getSchema()->getExpressionsInScope();
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalOrderBy>(expressionsToOrderBy, isAscOrders, children[0]->copy());
    }

private:
    binder::expression_vector expressionsToOrderBy;
    std::vector<bool> isAscOrders;
};

} // namespace planner
} // namespace kuzu
