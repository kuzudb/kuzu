#include "planner/operator/scan/logical_scan_internal_id.h"

namespace kuzu {
namespace planner {

void LogicalScanInternalID::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(internalID, groupPos);
}

void LogicalScanInternalID::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(internalID, 0);
}

} // namespace planner
} // namespace kuzu
