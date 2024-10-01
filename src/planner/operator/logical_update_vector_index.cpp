#include "planner/operator/logical_update_vector_index.h"


namespace kuzu {
namespace planner {

void LogicalUpdateVectorIndex::computeFlatSchema() {
    copyChildSchema(0);
    auto flatGroup = schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, flatGroup);
    }
}

void LogicalUpdateVectorIndex::computeFactorizedSchema() {
    copyChildSchema(0);
    schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, 0);
    }
}

} // namespace planner
} // namespace kuzu
