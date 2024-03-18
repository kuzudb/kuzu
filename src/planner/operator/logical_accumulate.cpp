#include "planner/operator/logical_accumulate.h"

#include "planner/operator/factorization/flatten_resolver.h"
#include "planner/operator/factorization/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalAccumulate::computeFactorizedSchema() {
    createEmptySchema();
    auto childSchema = children[0]->getSchema();
    SinkOperatorUtil::recomputeSchema(*childSchema, getExpressionsToAccumulate(), *schema);
    if (offset != nullptr) {
        // If we need to generate row offset. Then all expressions must have been flattened and
        // accumulated. So the new schema should just have one group.
        KU_ASSERT(schema->getNumGroups() == 1);
        schema->insertToGroupAndScope(offset, 0);
    }
}

void LogicalAccumulate::computeFlatSchema() {
    copyChildSchema(0);
    if (offset != nullptr) {
        schema->insertToGroupAndScope(offset, 0);
    }
}

f_group_pos_set LogicalAccumulate::getGroupPositionsToFlatten() const {
    f_group_pos_set result;
    auto childSchema = children[0]->getSchema();
    for (auto& expression : flatExprs) {
        auto groupPos = childSchema->getGroupPos(*expression);
        result.insert(groupPos);
    }
    return factorization::FlattenAll::getGroupsPosToFlatten(result, childSchema);
}

} // namespace planner
} // namespace kuzu
