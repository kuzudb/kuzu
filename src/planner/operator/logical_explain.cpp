#include "planner/operator/logical_explain.h"

#include "common/exception/not_implemented.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalExplain::computeSchema() {
    switch (explainType) {
    case ExplainType::PROFILE:
        copyChildSchema(0);
        break;
    case ExplainType::PHYSICAL_PLAN:
        createEmptySchema();
        break;
    default:
        throw NotImplementedException{"LogicalExplain::computeFlatSchema"};
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
