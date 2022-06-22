#pragma once

#include "bound_updating_clause.h"

namespace graphflow {
namespace binder {

struct BoundSetItem {
    BoundSetItem(shared_ptr<Expression> origin, shared_ptr<Expression> target)
        : origin{move(origin)}, target{move(target)} {}
    BoundSetItem(const BoundSetItem& other) : BoundSetItem{other.origin, other.target} {}

    shared_ptr<Expression> origin;
    shared_ptr<Expression> target;
};

class BoundSetClause : public BoundUpdatingClause {

public:
    BoundSetClause() : BoundUpdatingClause{ClauseType::SET} {};
    ~BoundSetClause() = default;

    inline void addSetItem(unique_ptr<BoundSetItem> boundSetItem) {
        boundSetItems.push_back(move(boundSetItem));
    }
    inline uint32_t getNumSetItems() const { return boundSetItems.size(); }
    inline BoundSetItem* getSetItem(uint32_t idx) const { return boundSetItems[idx].get(); }

    inline expression_vector getPropertiesToRead() const override {
        expression_vector result;
        for (auto& setItem : boundSetItems) {
            for (auto& property : setItem->target->getSubPropertyExpressions()) {
                result.push_back(property);
            }
        }
        return result;
    }

    inline unique_ptr<BoundUpdatingClause> copy() override {
        auto result = make_unique<BoundSetClause>();
        for (auto& setItem : boundSetItems) {
            result->addSetItem(make_unique<BoundSetItem>(*setItem));
        }
        return result;
    }

private:
    vector<unique_ptr<BoundSetItem>> boundSetItems;
};

} // namespace binder
} // namespace graphflow
