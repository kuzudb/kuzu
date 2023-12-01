#pragma once

#include "bound_return_clause.h"

namespace kuzu {
namespace binder {

/**
 * BoundWithClause may not have whereExpression
 */
class BoundWithClause : public BoundReturnClause {
public:
    explicit BoundWithClause(std::unique_ptr<BoundProjectionBody> projectionBody)
        : BoundReturnClause{std::move(projectionBody)} {}

    inline void setWhereExpression(std::shared_ptr<Expression> expression) {
        whereExpression = std::move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline std::shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

private:
    std::shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace kuzu
