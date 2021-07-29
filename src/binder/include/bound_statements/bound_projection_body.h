#pragma once

#include "src/binder/include/expression/expression.h"

namespace graphflow {
namespace binder {

class BoundProjectionBody {

public:
    explicit BoundProjectionBody(vector<shared_ptr<Expression>> projectionExpressions)
        : projectionExpressions{move(projectionExpressions)}, limitNumber{UINT64_MAX} {}

    inline void setLimitNumber(uint64_t number) { limitNumber = number; }

    inline const vector<shared_ptr<Expression>>& getProjectionExpressions() const {
        return projectionExpressions;
    }
    inline uint64_t getLimitNumber() const { return limitNumber; }

private:
    vector<shared_ptr<Expression>> projectionExpressions;
    uint64_t limitNumber;
};

} // namespace binder
} // namespace graphflow
