#include "planner/logical_plan/logical_operator/logical_union.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalUnion::computeSchema() {
    auto firstChildSchema = children[0]->getSchema();
    schema = make_unique<Schema>();
    SinkOperatorUtil::recomputeSchema(*firstChildSchema, *schema);
}

unique_ptr<LogicalOperator> LogicalUnion::copy() {
    vector<unique_ptr<Schema>> copiedSchemas;
    vector<shared_ptr<LogicalOperator>> copiedChildren;
    for (auto i = 0u; i < getNumChildren(); ++i) {
        copiedSchemas.push_back(schemasBeforeUnion[i]->copy());
        copiedChildren.push_back(getChild(i)->copy());
    }
    return make_unique<LogicalUnion>(
        expressionsToUnion, std::move(copiedSchemas), std::move(copiedChildren));
}

} // namespace planner
} // namespace kuzu
