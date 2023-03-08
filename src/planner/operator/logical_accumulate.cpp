#include "planner/logical_plan/logical_operator/logical_accumulate.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalAccumulate::computeFactorizedSchema() {
    createEmptySchema();
    auto childSchema = children[0]->getSchema();
    SinkOperatorUtil::recomputeSchema(*childSchema, getExpressions(), *schema);
}

void LogicalAccumulate::computeFlatSchema() {
    copyChildSchema(0);
}

} // namespace planner
} // namespace kuzu
