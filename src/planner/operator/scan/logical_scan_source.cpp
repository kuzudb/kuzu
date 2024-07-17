#include "planner/operator/scan/logical_scan_source.h"

namespace kuzu {
namespace planner {

void LogicalScanSource::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(info.columns, groupPos);
    if (offset != nullptr) {
        schema->insertToGroupAndScope(offset, groupPos);
    }
}

void LogicalScanSource::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(info.columns, 0);
    if (offset != nullptr) {
        schema->insertToGroupAndScope(offset, 0);
    }
}

} // namespace planner
} // namespace kuzu
