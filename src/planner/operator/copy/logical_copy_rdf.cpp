#include "planner/logical_plan/copy/logical_copy_rdf.h"

namespace kuzu {
namespace planner {

void LogicalCopyRDF::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(subjectExpression, groupPos);
    schema->insertToGroupAndScope(predicateExpression, groupPos);
    schema->insertToGroupAndScope(objectExpression, groupPos);
    schema->insertToGroupAndScope(offsetExpression, groupPos);
    schema->insertToGroupAndScope(outputExpression, groupPos);
    schema->setGroupAsSingleState(groupPos);
}

void LogicalCopyRDF::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(subjectExpression, 0);
    schema->insertToGroupAndScope(predicateExpression, 0);
    schema->insertToGroupAndScope(objectExpression, 0);
    schema->insertToGroupAndScope(offsetExpression, 0);
    schema->insertToGroupAndScope(outputExpression, 0);
}

} // namespace planner
} // namespace kuzu
