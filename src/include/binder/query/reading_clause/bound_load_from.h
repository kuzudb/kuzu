#pragma once

#include "binder/copy/bound_file_scan_info.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundLoadFrom : public BoundReadingClause {
public:
    explicit BoundLoadFrom(std::unique_ptr<BoundFileScanInfo> info)
        : BoundReadingClause{common::ClauseType::LOAD_FROM}, info{std::move(info)} {}
    BoundLoadFrom(const BoundLoadFrom& other)
        : BoundReadingClause{common::ClauseType::LOAD_FROM}, info{other.info->copy()},
          wherePredicate{other.wherePredicate} {}

    inline BoundFileScanInfo* getInfo() const { return info.get(); }

    inline void setWherePredicate(std::shared_ptr<Expression> expression) {
        wherePredicate = std::move(expression);
    }
    inline bool hasWherePredicate() const { return wherePredicate != nullptr; }
    inline std::shared_ptr<Expression> getWherePredicate() const { return wherePredicate; }
    inline expression_vector getPredicatesSplitOnAnd() const {
        return hasWherePredicate() ? wherePredicate->splitOnAND() : expression_vector{};
    }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundLoadFrom>(*this);
    }

private:
    std::unique_ptr<BoundFileScanInfo> info;
    std::shared_ptr<Expression> wherePredicate;
};

} // namespace binder
} // namespace kuzu
