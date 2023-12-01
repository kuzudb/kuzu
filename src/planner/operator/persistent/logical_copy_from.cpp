#include "planner/operator/persistent/logical_copy_from.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalCopyFrom::computeFactorizedSchema() {
    copyChildSchema(0);
    auto flatGroup = schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, flatGroup);
}

void LogicalCopyFrom::computeFlatSchema() {
    copyChildSchema(0);
    schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, 0);
}

} // namespace planner
} // namespace kuzu
