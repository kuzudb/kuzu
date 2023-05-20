#include "binder/query/updating_clause/bound_set_clause.h"

namespace kuzu {
namespace binder {

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
