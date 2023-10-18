#include "planner/operator/persistent/logical_copy_from.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalCopyFrom::computeFactorizedSchema() {
    copyChildSchema(0);
    // Assert that columns to be scanned have been inserted in group at pos 0
    assert(info->fileScanInfo->columns[0] == schema->getExpressionsInScope(0)[0]);
    schema->insertToGroupAndScope(info->nullColumns, 0);
    auto flatGroup = schema->createGroup();
    schema->insertToGroupAndScope(outputExpression, flatGroup);
}

void LogicalCopyFrom::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(info->nullColumns, 0);
    schema->insertToGroupAndScope(outputExpression, 0);
}

} // namespace planner
} // namespace kuzu
