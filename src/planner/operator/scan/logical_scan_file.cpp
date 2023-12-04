#include "planner/operator/scan/logical_scan_file.h"

namespace kuzu {
namespace planner {

void LogicalScanFile::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(info->columns, groupPos);
    schema->insertToGroupAndScope(info->offset, groupPos);
}

void LogicalScanFile::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(info->columns, 0);
    schema->insertToGroupAndScope(info->offset, 0);
}

} // namespace planner
} // namespace kuzu
