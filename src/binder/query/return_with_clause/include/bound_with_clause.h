#pragma once

#include "bound_return_clause.h"

namespace kuzu {
namespace binder {

/**
 * BoundWithClause may not have whereExpression
 */
class BoundWithClause : public BoundReturnClause {

public:
    explicit BoundWithClause(unique_ptr<BoundProjectionBody> projectionBody)
        : BoundReturnClause{move(projectionBody)} {}

    inline void setWhereExpression(shared_ptr<Expression> expression) {
        whereExpression = move(expression);
    }

    inline bool hasWhereExpression() const { return whereExpression != nullptr; }

    inline shared_ptr<Expression> getWhereExpression() const { return whereExpression; }

private:
    shared_ptr<Expression> whereExpression;
};

} // namespace binder
} // namespace kuzu
