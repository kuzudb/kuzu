#include "planner/logical_plan/logical_operator/logical_explain.h"

namespace kuzu {
namespace planner {

void LogicalExplain::computeSchema() {
    switch (explainType) {
    case common::ExplainType::PROFILE:
        copyChildSchema(0);
        break;
    case common::ExplainType::PHYSICAL_PLAN:
        createEmptySchema();
        break;
    default:
        throw common::NotImplementedException{"LogicalExplain::computeFlatSchema"};
    }
}

void LogicalExplain::computeFlatSchema() {
    computeSchema();
    schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, 0);
}

void LogicalExplain::computeFactorizedSchema() {
    computeSchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, groupPos);
    schema->setGroupAsSingleState(groupPos);
}

} // namespace planner
} // namespace kuzu
