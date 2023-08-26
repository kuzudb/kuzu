#include "planner/logical_plan/logical_schema_mapping.h"

namespace kuzu {
namespace planner {

void LogicalSchemaMapping::computeFactorizedSchema() {
    createEmptySchema();
    auto childSchema = children[0]->getSchema();
    for (int childGroupPos = 0; childGroupPos < childSchema->getNumGroups(); childGroupPos++) {
        auto childGroup = childSchema->getGroup(childGroupPos);
        auto groupPos = schema->createGroup();
        auto group = schema->getGroup(groupPos);
        for (const auto& expression : childGroup->getExpressions()) {
            auto newExpression = expression;
            if (expressionsMapping.contains(expression)) {
                newExpression = expressionsMapping[expression];
            }
            if (childSchema->isExpressionInScope(*expression)) {
                schema->insertToGroupAndScope(newExpression, groupPos);
            } else {
                group->insertExpression(newExpression);
            }
        }
    }
}

void LogicalSchemaMapping::computeFlatSchema() {
    createEmptySchema();
    auto childSchema = children[0]->getSchema();
    schema->createGroup();
    for (int childGroupPos = 0; childGroupPos < childSchema->getNumGroups(); childGroupPos++) {
        auto childGroup = childSchema->getGroup(childGroupPos);
        auto group = schema->getGroup(0);
        for (const auto& expression : childGroup->getExpressions()) {
            auto newExpression = expression;
            if (expressionsMapping.contains(expression)) {
                newExpression = expressionsMapping[expression];
            }
            if (childSchema->isExpressionInScope(*expression)) {
                schema->insertToGroupAndScope(newExpression, 0);
            } else {
                group->insertExpression(newExpression);
            }
        }
    }
}

} // namespace planner
} // namespace kuzu
