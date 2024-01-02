#pragma once

#include "binder/expression/expression.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundUnwindClause : public BoundReadingClause {
public:
    BoundUnwindClause(std::shared_ptr<Expression> inExpr, std::shared_ptr<Expression> outExpr)
        : BoundReadingClause{common::ClauseType::UNWIND}, inExpr{std::move(inExpr)},
          outExpr{std::move(outExpr)} {}

    inline std::shared_ptr<Expression> getInExpr() const { return inExpr; }
    inline std::shared_ptr<Expression> getOutExpr() const { return outExpr; }

private:
    std::shared_ptr<Expression> inExpr;
    std::shared_ptr<Expression> outExpr;
};
} // namespace binder
} // namespace kuzu
