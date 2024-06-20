#include "planner/operator/persistent/logical_create_vector_index.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalCreateVectorIndex::computeFlatSchema() {
    copyChildSchema(0);
    auto flatGroup = schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, flatGroup);
    }
}

void LogicalCreateVectorIndex::computeFactorizedSchema() {
    copyChildSchema(0);
    schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, 0);
    }
}

} // namespace planner
} // namespace kuzu
