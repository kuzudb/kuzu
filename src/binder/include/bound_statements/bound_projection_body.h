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
        : projectionExpressions{other.projectionExpressions}, skipNumber{other.skipNumber},
          limitNumber{other.limitNumber} {}

    inline void setSkipNumber(uint64_t number) { skipNumber = number; }
    inline void setLimitNumber(uint64_t number) { limitNumber = number; }

    inline const vector<shared_ptr<Expression>>& getProjectionExpressions() const {
        return projectionExpressions;
    }
    inline bool hasSkip() const { return skipNumber != UINT64_MAX; }
    inline bool hasLimit() const { return limitNumber != UINT64_MAX; }
    inline uint64_t getSkipNumber() const { return skipNumber; }
    inline uint64_t getLimitNumber() const { return limitNumber; }

    vector<shared_ptr<Expression>> getDependentProperties() const;

private:
    vector<shared_ptr<Expression>> projectionExpressions;
    uint64_t skipNumber;
    uint64_t limitNumber;
};

} // namespace binder
} // namespace graphflow
