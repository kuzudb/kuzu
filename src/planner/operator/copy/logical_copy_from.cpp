#include "planner/logical_plan/copy/logical_copy_from.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalCopyFrom::computeFactorizedSchema() {
    createEmptySchema();
    auto flatGroup = schema->createGroup();
    auto unflatGroup = schema->createGroup();
    schema->insertToGroupAndScope(info->columnExpressions, unflatGroup);
    if (info->tableSchema->tableType == TableType::REL) {
        schema->insertToGroupAndScope(info->boundOffsetExpression, unflatGroup);
        schema->insertToGroupAndScope(info->nbrOffsetExpression, unflatGroup);
    }
    schema->insertToGroupAndScope(info->offsetExpression, flatGroup);
    schema->insertToGroupAndScope(outputExpression, flatGroup);
    schema->setGroupAsSingleState(flatGroup);
}

void LogicalCopyFrom::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(info->columnExpressions, 0);
    if (info->tableSchema->tableType == TableType::REL) {
        schema->insertToGroupAndScope(info->boundOffsetExpression, 0);
        schema->insertToGroupAndScope(info->nbrOffsetExpression, 0);
    }
    schema->insertToGroupAndScope(info->offsetExpression, 0);
    schema->insertToGroupAndScope(outputExpression, 0);
}

} // namespace planner
} // namespace kuzu
