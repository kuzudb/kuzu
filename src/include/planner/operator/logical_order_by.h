#pragma once

#include "planner/operator/logical_operator.h"

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

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final;

    inline binder::expression_vector getExpressionsToOrderBy() const {
        return expressionsToOrderBy;
    }
    inline std::vector<bool> getIsAscOrders() const { return isAscOrders; }

    inline bool isTopK() const { return hasLimitNum(); }
    inline void setSkipNum(uint64_t num) { skipNum = num; }
    inline uint64_t getSkipNum() const { return skipNum; }
    inline void setLimitNum(uint64_t num) { limitNum = num; }
    inline bool hasLimitNum() const { return limitNum != UINT64_MAX; }
    inline uint64_t getLimitNum() const { return limitNum; }

    inline std::unique_ptr<LogicalOperator> copy() final {
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
