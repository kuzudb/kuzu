#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class BoundProjectionBody {
    static constexpr uint64_t INVALID_NUMBER = UINT64_MAX;

public:
    BoundProjectionBody(bool isDistinct, expression_vector projectionExpressions)
        : isDistinct{isDistinct}, projectionExpressions{std::move(projectionExpressions)},
          skipNumber{INVALID_NUMBER}, limitNumber{INVALID_NUMBER} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundProjectionBody);

    inline bool getIsDistinct() const { return isDistinct; }

    inline void setProjectionExpressions(expression_vector expressions) {
        projectionExpressions = std::move(expressions);
    }
    inline expression_vector getProjectionExpressions() const { return projectionExpressions; }

    inline void setGroupByExpressions(expression_vector expressions) {
        groupByExpressions = std::move(expressions);
    }
    inline expression_vector getGroupByExpressions() const { return groupByExpressions; }

    inline void setAggregateExpressions(expression_vector expressions) {
        aggregateExpressions = std::move(expressions);
    }
    inline bool hasAggregateExpressions() const { return !aggregateExpressions.empty(); }
    inline expression_vector getAggregateExpressions() const { return aggregateExpressions; }

    inline void setOrderByExpressions(expression_vector expressions, std::vector<bool> sortOrders) {
        orderByExpressions = std::move(expressions);
        isAscOrders = std::move(sortOrders);
    }
    inline bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
    inline const expression_vector& getOrderByExpressions() const { return orderByExpressions; }
    inline const std::vector<bool>& getSortingOrders() const { return isAscOrders; }

    inline void setSkipNumber(uint64_t number) { skipNumber = number; }
    inline bool hasSkip() const { return skipNumber != INVALID_NUMBER; }
    inline uint64_t getSkipNumber() const { return skipNumber; }

    inline void setLimitNumber(uint64_t number) { limitNumber = number; }
    inline bool hasLimit() const { return limitNumber != INVALID_NUMBER; }
    inline uint64_t getLimitNumber() const { return limitNumber; }

    inline bool hasSkipOrLimit() const { return hasSkip() || hasLimit(); }

private:
    BoundProjectionBody(const BoundProjectionBody& other)
        : isDistinct{other.isDistinct}, projectionExpressions{other.projectionExpressions},
          groupByExpressions{other.groupByExpressions},
          aggregateExpressions{other.aggregateExpressions},
          orderByExpressions{other.orderByExpressions}, isAscOrders{other.isAscOrders},
          skipNumber{other.skipNumber}, limitNumber{other.limitNumber} {}

private:
    bool isDistinct;
    expression_vector projectionExpressions;
    expression_vector groupByExpressions;
    expression_vector aggregateExpressions;
    expression_vector orderByExpressions;
    std::vector<bool> isAscOrders;
    uint64_t skipNumber;
    uint64_t limitNumber;
};

} // namespace binder
} // namespace kuzu
