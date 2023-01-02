#include "planner/logical_plan/logical_operator/logical_order_by.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalOrderBy::computeSchema() {
    auto childSchema = children[0]->getSchema();
    schema = make_unique<Schema>();
    SinkOperatorUtil::recomputeSchema(*childSchema, *schema);
}

string LogicalOrderBy::getExpressionsForPrinting() const {
    auto result = string();
    for (auto& expression : expressionsToOrderBy) {
        result += expression->getUniqueName() + ", ";
    }
    return result;
}

} // namespace planner
} // namespace kuzu
