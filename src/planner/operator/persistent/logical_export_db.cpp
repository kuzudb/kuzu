#include "planner/operator/persistent/logical_export_db.h"

namespace kuzu {
namespace planner {

void LogicalExportDatabase::computeFactorizedSchema() {
    createEmptySchema();
}

void LogicalExportDatabase::computeFlatSchema() {
    createEmptySchema();
}

} // namespace planner
} // namespace kuzu
