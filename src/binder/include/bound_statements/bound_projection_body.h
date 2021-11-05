#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class BoundProjectionBody {

public:
    explicit BoundProjectionBody(vector<shared_ptr<Expression>> projectionExpressions)
        : projectionExpressions{move(projectionExpressions)}, skipNumber{UINT64_MAX},
          limitNumber{UINT64_MAX} {}

    BoundProjectionBody(const BoundProjectionBody& other)
        : projectionExpressions{other.projectionExpressions},
          orderByExpressions{other.orderByExpressions}, isAscOrders{other.isAscOrders},
          skipNumber{other.skipNumber}, limitNumber{other.limitNumber} {}

    inline void setOrderByExpressions(
        vector<shared_ptr<Expression>> expressions, vector<bool> sortOrders) {
        orderByExpressions = move(expressions);
        isAscOrders = move(sortOrders);
    }
    inline void setSkipNumber(uint64_t number) { skipNumber = number; }
    inline void setLimitNumber(uint64_t number) { limitNumber = number; }

    bool hasAggregationExpressions() const;
    vector<shared_ptr<Expression>> getAggregationExpressions() const;
    inline vector<shared_ptr<Expression>> getProjectionExpressions() const {
        return projectionExpressions;
    }
    inline bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
    inline const vector<shared_ptr<Expression>>& getOrderByExpressions() const {
        return orderByExpressions;
    }
    inline const vector<bool>& getSortingOrders() const { return isAscOrders; }
    inline bool hasSkip() const { return skipNumber != UINT64_MAX; }
    inline bool hasLimit() const { return limitNumber != UINT64_MAX; }
    inline uint64_t getSkipNumber() const { return skipNumber; }
    inline uint64_t getLimitNumber() const { return limitNumber; }

private:
    vector<shared_ptr<Expression>> projectionExpressions;
    vector<shared_ptr<Expression>> orderByExpressions;
    vector<bool> isAscOrders;
    uint64_t skipNumber;
    uint64_t limitNumber;
};

} // namespace binder
} // namespace graphflow
