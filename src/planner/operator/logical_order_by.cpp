#include "planner/logical_plan/logical_operator/logical_order_by.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalOrderBy::computeSchema() {
    auto childSchema = children[0]->getSchema();
    schema = std::make_unique<Schema>();
    SinkOperatorUtil::recomputeSchema(*childSchema, expressionsToMaterialize, *schema);
}

} // namespace planner
} // namespace kuzu
