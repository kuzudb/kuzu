#include "planner/operator/persistent/logical_import_db.h"

namespace kuzu {
namespace planner {

void LogicalImportDatabase::computeFactorizedSchema() {
    createEmptySchema();
}

void LogicalImportDatabase::computeFlatSchema() {
    createEmptySchema();
}

} // namespace planner
} // namespace kuzu
