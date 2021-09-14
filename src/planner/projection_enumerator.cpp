#include "src/planner/include/projection_enumerator.h"

#include "src/planner/include/enumerator.h"
#include "src/planner/include/logical_plan/operator/aggregate/logical_aggregate.h"
#include "src/planner/include/logical_plan/operator/limit/logical_limit.h"
#include "src/planner/include/logical_plan/operator/multiplicity_reducer/logical_multiplcity_reducer.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/skip/logical_skip.h"

namespace graphflow {
namespace planner {

void ProjectionEnumerator::enumerateProjectionBody(const BoundProjectionBody& projectionBody,
    const vector<unique_ptr<LogicalPlan>>& plans, bool isFinalReturn) {
    for (auto& plan : plans) {
        if (projectionBody.hasAggregationExpressions()) {
            appendAggregate(projectionBody.getAggregationExpressions(), *plan);
        } else {
            appendProjection(projectionBody.getProjectionExpressions(), *plan, isFinalReturn);
        }
        if (projectionBody.hasSkip() || projectionBody.hasLimit()) {
            appendMultiplicityReducer(*plan);
            if (projectionBody.hasSkip()) {
                appendSkip(projectionBody.getSkipNumber(), *plan);
            }
            if (projectionBody.hasLimit()) {
                appendLimit(projectionBody.getLimitNumber(), *plan);
            }
        }
    }
}

void ProjectionEnumerator::appendProjection(const vector<shared_ptr<Expression>>& expressions,
    LogicalPlan& plan, bool isRewritingAllProperties) {
    // extract and rewrite expressions to project
    vector<shared_ptr<Expression>> expressionsToProject;
    for (auto& expression : expressions) {
        auto expressionRemovingAlias =
            expression->expressionType == ALIAS ? expression->removeAlias() : expression;
        if (expressionRemovingAlias->expressionType == VARIABLE) {
            for (auto& property :
                rewriteVariableExpression(expressionRemovingAlias, isRewritingAllProperties)) {
                expressionsToProject.push_back(property);
            }
        } else {
            expressionsToProject.push_back(expression);
        }
    }

    // every expression is written to a factorization group
    unordered_map<uint32_t, vector<shared_ptr<Expression>>> groupPosToExpressionsMap;
    for (auto& expression : expressionsToProject) {
        uint32_t groupPosToWrite =
            enumerator->appendScanPropertiesFlattensAndPlanSubqueryIfNecessary(*expression, plan);
        if (!groupPosToExpressionsMap.contains(groupPosToWrite)) {
            groupPosToExpressionsMap.insert({groupPosToWrite, vector<shared_ptr<Expression>>()});
        }
        groupPosToExpressionsMap.at(groupPosToWrite).push_back(expression);
    }

    // We collect the discarded group positions in the input data chunks to obtain their
    // multiplicity in the projection operation.
    vector<uint32_t> discardedGroupsPos;
    for (auto i = 0u; i < plan.schema->groups.size(); ++i) {
        if (!groupPosToExpressionsMap.contains(i)) {
            discardedGroupsPos.push_back(i);
        }
    }

    // reconstruct schema
    auto schemaBeforeProjection = plan.schema->copy();
    plan.schema->clearGroups();
    for (auto& [groupPosBeforeProjection, expressionsToProject] : groupPosToExpressionsMap) {
        auto newGroupPos = plan.schema->createGroup();
        for (auto& expressionToProject : expressionsToProject) {
            plan.schema->insertToGroup(expressionToProject->getInternalName(), newGroupPos);
        }
        // copy other information
        auto newGroup = plan.schema->groups[newGroupPos].get();
        newGroup->isFlat = schemaBeforeProjection->groups[groupPosBeforeProjection]->isFlat;
        newGroup->estimatedCardinality =
            schemaBeforeProjection->groups[groupPosBeforeProjection]->estimatedCardinality;
    }

    auto projection = make_shared<LogicalProjection>(move(expressionsToProject),
        move(schemaBeforeProjection), move(discardedGroupsPos), plan.lastOperator);
    plan.appendOperator(move(projection));
}

void ProjectionEnumerator::appendAggregate(
    const vector<shared_ptr<Expression>>& expressions, LogicalPlan& plan) {
    auto aggregate =
        make_shared<LogicalAggregate>(expressions, plan.schema->copy(), plan.lastOperator);
    plan.schema->clearGroups();
    auto groupPos = plan.schema->createGroup();
    for (auto& expression : expressions) {
        plan.schema->insertToGroup(expression->getInternalName(), groupPos);
    }
    plan.appendOperator(move(aggregate));
}

void ProjectionEnumerator::appendMultiplicityReducer(LogicalPlan& plan) {
    plan.appendOperator(make_shared<LogicalMultiplicityReducer>(plan.lastOperator));
}

void ProjectionEnumerator::appendLimit(uint64_t limitNumber, LogicalPlan& plan) {
    auto unFlatGroupsPos = plan.schema->getUnFlatGroupsPos();
    auto groupPosToSelect = unFlatGroupsPos.empty() ?
                                plan.schema->getAnyGroupPos() :
                                enumerator->appendFlattensButOne(unFlatGroupsPos, plan);
    // Because our resultSet is shared through the plan and limit might not appear at the end (due
    // to WITH clause), limit needs to know how many tuples are available under it. So it requires a
    // subset of dataChunks that may different from shared resultSet.
    vector<uint32_t> groupsPosToLimit;
    for (auto i = 0u; i < plan.schema->groups.size(); ++i) {
        groupsPosToLimit.push_back(i);
    }
    auto limit = make_shared<LogicalLimit>(
        limitNumber, groupPosToSelect, move(groupsPosToLimit), plan.lastOperator);
    for (auto& group : plan.schema->groups) {
        group->estimatedCardinality = limitNumber;
    }
    plan.appendOperator(move(limit));
}

void ProjectionEnumerator::appendSkip(uint64_t skipNumber, LogicalPlan& plan) {
    auto unFlatGroupsPos = plan.schema->getUnFlatGroupsPos();
    auto groupPosToSelect = unFlatGroupsPos.empty() ?
                                plan.schema->getAnyGroupPos() :
                                enumerator->appendFlattensButOne(unFlatGroupsPos, plan);
    vector<uint32_t> groupsPosToSkip;
    for (auto i = 0u; i < plan.schema->groups.size(); ++i) {
        groupsPosToSkip.push_back(i);
    }
    auto skip = make_shared<LogicalSkip>(
        skipNumber, groupPosToSelect, move(groupsPosToSkip), plan.lastOperator);
    for (auto& group : plan.schema->groups) {
        group->estimatedCardinality -= skipNumber;
    }
    plan.appendOperator(move(skip));
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteVariableExpression(
    const shared_ptr<Expression>& variable, bool isRewritingAllProperties) {
    GF_ASSERT(VARIABLE == variable->expressionType);
    if (NODE == variable->dataType) {
        return rewriteNodeExpression(
            static_pointer_cast<NodeExpression>(variable), isRewritingAllProperties);
    } else {
        assert(REL == variable->dataType);
        return isRewritingAllProperties ?
                   rewriteRelExpression(static_pointer_cast<RelExpression>(variable)) :
                   vector<shared_ptr<Expression>>();
    }
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteNodeExpression(
    const shared_ptr<NodeExpression>& node, bool isRewritingAllProperties) {
    vector<shared_ptr<Expression>> result;
    result.emplace_back(
        make_shared<PropertyExpression>(NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX, node));
    if (isRewritingAllProperties) {
        for (auto& property :
            createPropertyExpressions(node, catalog.getAllNodeProperties(node->label))) {
            result.push_back(property);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteRelExpression(
    const shared_ptr<RelExpression>& rel) {
    vector<shared_ptr<Expression>> result;
    for (auto& property : createPropertyExpressions(rel, catalog.getRelProperties(rel->label))) {
        result.push_back(property);
    }
    return result;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::createPropertyExpressions(
    const shared_ptr<Expression>& variable, const vector<PropertyDefinition>& properties) {
    vector<shared_ptr<Expression>> result;
    for (auto& property : properties) {
        result.emplace_back(make_shared<PropertyExpression>(
            property.dataType, property.name, property.id, variable));
    }
    return result;
}

} // namespace planner
} // namespace graphflow
