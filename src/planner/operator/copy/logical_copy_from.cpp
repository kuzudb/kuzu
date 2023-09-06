#include "planner/logical_plan/copy/logical_copy_from.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void LogicalCopyFrom::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(info->columnExpressions, groupPos);
    if (info->tableSchema->tableType == TableType::REL) {
        schema->insertToGroupAndScope(info->boundOffsetExpression, groupPos);
        schema->insertToGroupAndScope(info->nbrOffsetExpression, groupPos);
    }
    schema->insertToGroupAndScope(info->offsetExpression, groupPos);
    schema->insertToGroupAndScope(outputExpression, groupPos);
    schema->setGroupAsSingleState(groupPos);
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
