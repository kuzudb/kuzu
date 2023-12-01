#include "planner/operator/logical_cross_product.h"

#include "planner/operator/factorization/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalCrossProduct::computeFactorizedSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    SinkOperatorUtil::mergeSchema(*buildSchema, buildSchema->getExpressionsInScope(), *schema);
}

void LogicalCrossProduct::computeFlatSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    KU_ASSERT(schema->getNumGroups() == 1);
    for (auto& expression : buildSchema->getExpressionsInScope()) {
        schema->insertToGroupAndScope(expression, 0);
    }
}

} // namespace planner
} // namespace kuzu
