#include "planner/operator/persistent/logical_merge.h"

#include "planner/operator/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalMerge::getGroupsPosToFlatten() {
    auto childSchema = children[0]->getSchema();
    return factorization::FlattenAll::getGroupsPosToFlatten(
        childSchema->getGroupsPosInScope(), childSchema);
}

} // namespace planner
} // namespace kuzu
