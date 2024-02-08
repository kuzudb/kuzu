#include "planner/operator/persistent/logical_export_db.h"

#include "planner/operator/factorization/flatten_resolver.h"

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
