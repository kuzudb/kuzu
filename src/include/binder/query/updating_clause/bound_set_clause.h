#pragma once

#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundSetClause : public BoundUpdatingClause {

public:
    BoundSetClause() : BoundUpdatingClause{ClauseType::SET} {};
    ~BoundSetClause() override = default;

    inline void addSetItem(expression_pair setItem) { setItems.push_back(std::move(setItem)); }

    inline expression_vector getPropertiesToRead() const override {
        expression_vector result;
        for (auto& setItem : setItems) {
            for (auto& property : setItem.second->getSubPropertyExpressions()) {
                result.push_back(property);
            }
        }
        return result;
    }

    inline vector<expression_pair> getSetItems() const { return setItems; }

    inline unique_ptr<BoundUpdatingClause> copy() override {
        auto result = make_unique<BoundSetClause>();
        for (auto& setItem : setItems) {
            result->addSetItem(setItem);
        }
        return result;
    }

private:
    vector<expression_pair> setItems;
};

} // namespace binder
} // namespace kuzu
