#include "planner/operator/persistent/logical_copy_from.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalCopyFrom::computeFactorizedSchema() {
    copyChildSchema(0);
    auto flatGroup = schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, flatGroup);
    }
}

void LogicalCopyFrom::computeFlatSchema() {
    copyChildSchema(0);
    schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, 0);
    }
}

} // namespace planner
} // namespace kuzu
