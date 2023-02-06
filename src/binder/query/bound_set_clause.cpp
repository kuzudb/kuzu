#include "binder/query/updating_clause/bound_set_clause.h"

namespace kuzu {
namespace binder {

expression_vector BoundSetClause::getPropertiesToRead() const {
    expression_vector result;
    for (auto& setNodeProperty : setNodeProperties) {
        for (auto& property : setNodeProperty->getSetItem().second->getSubPropertyExpressions()) {
            result.push_back(property);
        }
    }
    for (auto& setRelProperty : setRelProperties) {
        for (auto& property : setRelProperty->getSetItem().second->getSubPropertyExpressions()) {
            result.push_back(property);
        }
    }
    return result;
}

std::unique_ptr<BoundUpdatingClause> BoundSetClause::copy() {
    auto result = std::make_unique<BoundSetClause>();
    for (auto& setNodeProperty : setNodeProperties) {
        result->addSetNodeProperty(setNodeProperty->copy());
    }
    for (auto& setRelProperty : setRelProperties) {
        result->addSetRelProperty(setRelProperty->copy());
    }
    return result;
}

} // namespace binder
} // namespace kuzu
