#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class BoundProjectionBody {
public:
    explicit BoundProjectionBody(bool isDistinct, expression_vector projectionExpressions)
        : isDistinct{isDistinct}, projectionExpressions{std::move(projectionExpressions)},
          skipNumber{UINT64_MAX}, limitNumber{UINT64_MAX} {}

    BoundProjectionBody(const BoundProjectionBody& other)
        : isDistinct{other.isDistinct}, projectionExpressions{other.projectionExpressions},
          orderByExpressions{other.orderByExpressions}, isAscOrders{other.isAscOrders},
          skipNumber{other.skipNumber}, limitNumber{other.limitNumber} {}

    ~BoundProjectionBody() = default;

    inline bool getIsDistinct() const { return isDistinct; }

    inline expression_vector getProjectionExpressions() const { return projectionExpressions; }

    bool hasAggregationExpressions() const;

    void setOrderByExpressions(expression_vector expressions, std::vector<bool> sortOrders);

    inline bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }

    inline const expression_vector& getOrderByExpressions() const { return orderByExpressions; }

    inline const std::vector<bool>& getSortingOrders() const { return isAscOrders; }

    inline void setSkipNumber(uint64_t number) { skipNumber = number; }

    inline bool hasSkip() const { return skipNumber != UINT64_MAX; }

    inline uint64_t getSkipNumber() const { return skipNumber; }

    inline void setLimitNumber(uint64_t number) { limitNumber = number; }

    inline bool hasLimit() const { return limitNumber != UINT64_MAX; }

    inline uint64_t getLimitNumber() const { return limitNumber; }

    inline bool hasSkipOrLimit() const { return hasSkip() || hasLimit(); }

    inline std::unique_ptr<BoundProjectionBody> copy() const {
        return std::make_unique<BoundProjectionBody>(*this);
    }

private:
    bool isDistinct;
    expression_vector projectionExpressions;
    expression_vector orderByExpressions;
    std::vector<bool> isAscOrders;
    uint64_t skipNumber;
    uint64_t limitNumber;
};

} // namespace binder
} // namespace kuzu
