#include "planner/operator/persistent/logical_copy_from.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalCopyFrom::computeFactorizedSchema() {
    switch (info.tableEntry->getTableType()) {
    case TableType::RDF: {
        createEmptySchema();
    } break;
    default:
        copyChildSchema(0);
    }
    auto flatGroup = schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, flatGroup);
    }
}

void LogicalCopyFrom::computeFlatSchema() {
    switch (info.tableEntry->getTableType()) {
    case TableType::RDF: {
        createEmptySchema();
    } break;
    default:
        copyChildSchema(0);
    }
    schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, 0);
    }
}

} // namespace planner
} // namespace kuzu
