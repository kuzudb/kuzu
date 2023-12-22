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
    BoundUnwindClause(const BoundUnwindClause& other)
        : BoundReadingClause{other}, inExpr{other.inExpr}, outExpr{other.outExpr} {}

    inline std::shared_ptr<Expression> getInExpr() const { return inExpr; }
    inline std::shared_ptr<Expression> getOutExpr() const { return outExpr; }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundUnwindClause>(*this);
    }

private:
    std::shared_ptr<Expression> inExpr;
    std::shared_ptr<Expression> outExpr;
};
} // namespace binder
} // namespace kuzu
