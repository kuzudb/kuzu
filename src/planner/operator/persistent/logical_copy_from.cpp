#include "planner/operator/persistent/logical_copy_from.h"

#include <planner/operator/factorization/flatten_resolver.h>

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalCopyFrom::computeFactorizedSchema() {
    copyChildSchema(0);
    auto flatGroup = schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, flatGroup);
    }
}

void LogicalCopyFrom::computeFlatSchema() {
    copyChildSchema(0);
    schema->createGroup();
    for (auto& expr : outExprs) {
        schema->insertToGroupAndScope(expr, 0);
    }
}

f_group_pos_set LogicalCopyFrom::getGroupsPosToFlatten() const {
    f_group_pos_set result;
    if (info.tableEntry->getTableType() == TableType::NODE) {
        auto childSchema = children[0]->getSchema();
        if (childSchema->getNumGroups() > 1) {
            result = FlattenAll::getGroupsPosToFlatten(childSchema->getExpressionsInScope(),
                *childSchema);
        }
    }
    return result;
}

} // namespace planner
} // namespace kuzu
