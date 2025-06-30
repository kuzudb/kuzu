#include "planner/operator/logical_explain.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalExplain::computeSchema() {
    switch (explainType) {
    case ExplainType::PROFILE:
        copyChildSchema(0);
        break;
    case ExplainType::PHYSICAL_PLAN:
    case ExplainType::LOGICAL_PLAN:
        createEmptySchema();
        break;
    default:
        KU_UNREACHABLE;
    }
}

void LogicalExplain::computeFlatSchema() {
    computeSchema();
}

void LogicalExplain::computeFactorizedSchema() {
    computeSchema();
}

} // namespace planner
} // namespace kuzu
