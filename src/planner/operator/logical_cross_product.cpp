#include "planner/logical_plan/logical_operator/logical_cross_product.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalCrossProduct::computeSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    SinkOperatorUtil::mergeSchema(*buildSchema, buildSchema->getExpressionsInScope(), *schema);
}

} // namespace planner
} // namespace kuzu
