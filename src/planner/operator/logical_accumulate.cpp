#include "planner/logical_plan/logical_operator/logical_accumulate.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalAccumulate::computeSchema() {
    auto childSchema = children[0]->getSchema();
    createEmptySchema();
    SinkOperatorUtil::recomputeSchema(*childSchema, expressions, *schema);
}

} // namespace planner
} // namespace kuzu
