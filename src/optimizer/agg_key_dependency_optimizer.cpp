#include "optimizer/agg_key_dependency_optimizer.h"

#include "binder/expression/node_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "planner/logical_plan/logical_aggregate.h"
#include "planner/logical_plan/logical_distinct.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void AggKeyDependencyOptimizer::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void AggKeyDependencyOptimizer::visitOperator(planner::LogicalOperator* op) {
    // bottom up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
    visitOperatorSwitch(op);
}

void AggKeyDependencyOptimizer::visitAggregate(planner::LogicalOperator* op) {
    auto agg = (LogicalAggregate*)op;
    auto [keyExpressions, payloadExpressions] =
        resolveKeysAndDependentKeys(agg->getKeyExpressions());
    agg->setKeyExpressions(keyExpressions);
    agg->setDependentKeyExpressions(payloadExpressions);
}

void AggKeyDependencyOptimizer::visitDistinct(planner::LogicalOperator* op) {
    auto distinct = (LogicalDistinct*)op;
    auto [keyExpressions, payloadExpressions] =
        resolveKeysAndDependentKeys(distinct->getKeyExpressions());
    distinct->setKeyExpressions(keyExpressions);
    distinct->setDependentKeyExpressions(payloadExpressions);
}

std::pair<binder::expression_vector, binder::expression_vector>
AggKeyDependencyOptimizer::resolveKeysAndDependentKeys(const binder::expression_vector& keys) {
    // Consider example RETURN a.ID, a.age, COUNT(*).
    // We first collect a.ID into primaryKeys. Then collect "a" into primaryVarNames.
    // Finally, we loop through all group by keys to put non-primary key properties under name "a"
    // into dependentKeyExpressions.

    // Collect primary keys from group keys.
    std::vector<binder::PropertyExpression*> primaryKeys;
    for (auto& expression : keys) {
        if (expression->expressionType == PROPERTY) {
            auto propertyExpression = (binder::PropertyExpression*)expression.get();
            if (propertyExpression->isPrimaryKey() || propertyExpression->isInternalID()) {
                primaryKeys.push_back(propertyExpression);
            }
        }
    }
    // Collect variable names whose primary key is part of group keys.
    std::unordered_set<std::string> primaryVarNames;
    for (auto& primaryKey : primaryKeys) {
        primaryVarNames.insert(primaryKey->getVariableName());
    }
    binder::expression_vector groupExpressions;
    binder::expression_vector dependentExpressions;
    for (auto& expression : keys) {
        if (expression->expressionType == PROPERTY) {
            auto propertyExpression = (binder::PropertyExpression*)expression.get();
            if (propertyExpression->isPrimaryKey() || propertyExpression->isInternalID()) {
                groupExpressions.push_back(expression);
            } else if (primaryVarNames.contains(propertyExpression->getVariableName())) {
                dependentExpressions.push_back(expression);
            } else {
                groupExpressions.push_back(expression);
            }
        } else if (ExpressionUtil::isNodeVariable(*expression)) {
            dependentExpressions.push_back(expression);
        } else if (ExpressionUtil::isRelVariable(*expression)) {
            dependentExpressions.push_back(expression);
        } else {
            groupExpressions.push_back(expression);
        }
    }
    return std::make_pair(std::move(groupExpressions), std::move(dependentExpressions));
}

} // namespace optimizer
} // namespace kuzu
