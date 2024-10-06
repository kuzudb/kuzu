#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class BoundProjectionBody {
    static constexpr uint64_t INVALID_NUMBER = UINT64_MAX;

public:
    explicit BoundProjectionBody(bool distinct)
        : distinct{distinct}, skipNumber{INVALID_NUMBER}, limitNumber{INVALID_NUMBER} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundProjectionBody);

    bool isDistinct() const { return distinct; }

    void setProjectionExpressions(expression_vector expressions) {
        projectionExpressions = std::move(expressions);
    }
    expression_vector getProjectionExpressions() const { return projectionExpressions; }

    void setGroupByExpressions(expression_vector expressions) {
        groupByExpressions = std::move(expressions);
    }
    expression_vector getGroupByExpressions() const { return groupByExpressions; }

    void setAggregateExpressions(expression_vector expressions) {
        aggregateExpressions = std::move(expressions);
    }
    bool hasAggregateExpressions() const { return !aggregateExpressions.empty(); }
    expression_vector getAggregateExpressions() const { return aggregateExpressions; }

    void setOrderByExpressions(expression_vector expressions, std::vector<bool> sortOrders) {
        orderByExpressions = std::move(expressions);
        isAscOrders = std::move(sortOrders);
    }
    bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
    const expression_vector& getOrderByExpressions() const { return orderByExpressions; }
    const std::vector<bool>& getSortingOrders() const { return isAscOrders; }

    void setSkipNumber(uint64_t number) { skipNumber = number; }
    bool hasSkip() const { return skipNumber != INVALID_NUMBER; }
    uint64_t getSkipNumber() const { return skipNumber; }

    void setLimitNumber(uint64_t number) { limitNumber = number; }
    bool hasLimit() const { return limitNumber != INVALID_NUMBER; }
    uint64_t getLimitNumber() const { return limitNumber; }

    bool hasSkipOrLimit() const { return hasSkip() || hasLimit(); }

private:
    BoundProjectionBody(const BoundProjectionBody& other)
        : distinct{other.distinct}, projectionExpressions{other.projectionExpressions},
          groupByExpressions{other.groupByExpressions},
          aggregateExpressions{other.aggregateExpressions},
          orderByExpressions{other.orderByExpressions}, isAscOrders{other.isAscOrders},
          skipNumber{other.skipNumber}, limitNumber{other.limitNumber} {}

private:
    bool distinct;
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
