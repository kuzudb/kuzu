#pragma once

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace binder {

struct BoundSetItem {
    BoundSetItem(shared_ptr<Expression> origin, shared_ptr<Expression> target)
        : origin{move(origin)}, target{move(target)} {}
    BoundSetItem(const BoundSetItem& other) : BoundSetItem{other.origin, other.target} {}

    shared_ptr<Expression> origin;
    shared_ptr<Expression> target;
};

class BoundSetClause {

public:
    BoundSetClause() = default;
    BoundSetClause(const BoundSetClause& other) {
        for (auto& setItem : other.boundSetItems) {
            addSetItem(make_unique<BoundSetItem>(*setItem));
        }
    }

    ~BoundSetClause() = default;

    inline void addSetItem(unique_ptr<BoundSetItem> boundSetItem) {
        boundSetItems.push_back(move(boundSetItem));
    }
    inline uint32_t getNumSetItems() const { return boundSetItems.size(); }
    inline BoundSetItem* getSetItem(uint32_t idx) const { return boundSetItems[idx].get(); }

    inline expression_vector getPropertiesToRead() const {
        expression_vector result;
        for (auto& setItem : boundSetItems) {
            for (auto& property : setItem->target->getSubPropertyExpressions()) {
                result.push_back(property);
            }
        }
        return result;
    }

private:
    vector<unique_ptr<BoundSetItem>> boundSetItems;
};

} // namespace binder
} // namespace graphflow
