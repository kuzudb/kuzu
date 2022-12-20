#include "planner/logical_plan/logical_operator/logical_hash_join.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalHashJoin::computeSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    if (joinType != JoinType::MARK) {
        vector<string> keys;
        for (auto& joinNode : joinNodes) {
            keys.push_back(joinNode->getInternalIDPropertyName());
        }
        SinkOperatorUtil::mergeSchema(*buildSchema, *schema, keys);
    } else {
        schema->insertToGroupAndScope(mark, markPos);
    }
}

string LogicalHashJoin::getExpressionsForPrinting() const {
    expression_vector expressions;
    for (auto& joinNode : joinNodes) {
        expressions.push_back(joinNode);
    }
    return ExpressionUtil::toString(expressions);
}

} // namespace planner
} // namespace kuzu
