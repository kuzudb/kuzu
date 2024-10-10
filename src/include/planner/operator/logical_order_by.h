#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalOrderBy final : public LogicalOperator {
public:
    LogicalOrderBy(binder::expression_vector expressionsToOrderBy, std::vector<bool> sortOrders,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ORDER_BY, std::move(child)},
          expressionsToOrderBy{std::move(expressionsToOrderBy)},
          isAscOrders{std::move(sortOrders)} {}

    f_group_pos_set getGroupsPosToFlatten();

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override;

    binder::expression_vector getExpressionsToOrderBy() const { return expressionsToOrderBy; }
    std::vector<bool> getIsAscOrders() const { return isAscOrders; }

    bool isTopK() const { return hasLimitNum(); }
    void setSkipNum(uint64_t num) { skipNum = num; }
    uint64_t getSkipNum() const { return skipNum; }
    void setLimitNum(uint64_t num) { limitNum = num; }
    bool hasLimitNum() const { return limitNum != UINT64_MAX; }
    uint64_t getLimitNum() const { return limitNum; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalOrderBy>(expressionsToOrderBy, isAscOrders, children[0]->copy());
    }

private:
    binder::expression_vector expressionsToOrderBy;
    std::vector<bool> isAscOrders;
    uint64_t skipNum = UINT64_MAX;
    uint64_t limitNum = UINT64_MAX;
};

} // namespace planner
} // namespace kuzu
