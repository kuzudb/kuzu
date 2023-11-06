#include "planner/operator/scan/logical_scan_file.h"

namespace kuzu {
namespace planner {

void LogicalScanFile::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(info->columns, groupPos);
    if (info->internalID != nullptr) {
        schema->insertToGroupAndScope(info->internalID, groupPos);
    }
}

void LogicalScanFile::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(info->columns, 0);
    if (info->internalID != nullptr) {
        schema->insertToGroupAndScope(info->internalID, 0);
    }
}

} // namespace planner
} // namespace kuzu
