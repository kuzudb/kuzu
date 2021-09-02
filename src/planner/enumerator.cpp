#include "src/planner/include/enumerator.h"

#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/limit/logical_limit.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
#include "src/planner/include/logical_plan/operator/multiplicity_reducer/logical_multiplcity_reducer.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/planner/include/logical_plan/operator/skip/logical_skip.h"

namespace graphflow {
namespace planner {

static unordered_set<uint32_t> getUnFlatGroupsPos(Expression& expression, const Schema& schema);
static unordered_set<uint32_t> getUnFlatGroupsPos(const Schema& schema);
static uint32_t getAnyGroupPos(Expression& expression, const Schema& schema);
static uint32_t getAnyGroupPos(const Schema& schema);

// check for all variable expressions and rewrite
static vector<shared_ptr<Expression>> rewriteExpressionToProject(
    vector<shared_ptr<Expression>> expressions, const Catalog& catalog,
    bool isRewritingAllProperties);
// rewrite variable as all of its property
static vector<shared_ptr<Expression>> rewriteVariableExpression(
    shared_ptr<Expression> expression, const Catalog& catalog, bool isRewritingAllProperties);
static vector<shared_ptr<Expression>> createLogicalPropertyExpressions(
    shared_ptr<Expression> expression, const vector<PropertyDefinition>& properties);

unique_ptr<LogicalPlan> Enumerator::getBestPlan(const BoundSingleQuery& singleQuery) {
    auto plans = enumeratePlans(singleQuery);
    unique_ptr<LogicalPlan> bestPlan = move(plans[0]);
    for (auto i = 1u; i < plans.size(); ++i) {
        if (plans[i]->cost < bestPlan->cost) {
            bestPlan = move(plans[i]);
        }
    }
    return bestPlan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans(const BoundSingleQuery& singleQuery) {
    auto normalizedQuery = QueryNormalizer::normalizeQuery(singleQuery);
    vector<unique_ptr<LogicalPlan>> plans;
    plans.push_back(make_unique<LogicalPlan>());
    for (auto& queryPart : normalizedQuery->getQueryParts()) {
        plans = enumerateQueryPart(*queryPart, move(plans));
    }
    return plans;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumerateQueryPart(
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    auto plans = move(prevPlans);
    if (queryPart.hasLoadCSVStatement()) {
        assert(plans.size() == 1); // load csv must be the first clause
        appendLoadCSV(*queryPart.getLoadCSVStatement(), *plans[0]);
    }
    plans = joinOrderEnumerator.enumerateJoinOrder(queryPart, move(plans));
    enumerateProjectionBody(*queryPart.getProjectionBody(), plans, queryPart.isLastQueryPart());
    return plans;
}

void Enumerator::enumerateProjectionBody(const BoundProjectionBody& projectionBody,
    const vector<unique_ptr<LogicalPlan>>& plans, bool isFinalReturn) {
    for (auto& plan : plans) {
        appendProjection(projectionBody.getProjectionExpressions(), *plan, isFinalReturn);
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

void Enumerator::appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan) {
    auto loadCSV = make_shared<LogicalLoadCSV>(loadCSVStatement.filePath,
        loadCSVStatement.tokenSeparator, loadCSVStatement.csvColumnVariables);
    auto groupPos = plan.schema->createGroup();
    for (auto& expression : loadCSVStatement.csvColumnVariables) {
        plan.schema->appendToGroup(expression->getInternalName(), groupPos);
    }
    plan.appendOperator(move(loadCSV));
}

uint32_t Enumerator::appendFlattensIfNecessary(
    const unordered_set<uint32_t>& unFlatGroupsPos, LogicalPlan& plan) {
    // randomly pick the first unFlat group to select and flatten the rest
    auto groupPosToSelect = *unFlatGroupsPos.begin();
    for (auto& unFlatGroupPos : unFlatGroupsPos) {
        if (unFlatGroupPos != groupPosToSelect) {
            appendFlattenIfNecessary(unFlatGroupPos, plan);
        }
    }
    return groupPosToSelect;
}

void Enumerator::appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan) {
    auto& group = *plan.schema->groups[groupPos];
    if (group.isFlat) {
        return;
    }
    plan.schema->flattenGroup(groupPos);
    plan.cost += group.estimatedCardinality;
    auto flatten = make_shared<LogicalFlatten>(group.getAnyVariable(), plan.lastOperator);
    plan.appendOperator(move(flatten));
}

void Enumerator::appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan) {
    appendScanPropertiesIfNecessary(*expression, plan);
    auto unFlatGroupsPos = getUnFlatGroupsPos(*expression, *plan.schema);
    uint32_t groupPosToSelect = unFlatGroupsPos.empty() ?
                                    getAnyGroupPos(*expression, *plan.schema) :
                                    appendFlattensIfNecessary(unFlatGroupsPos, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.lastOperator);
    plan.schema->groups[groupPosToSelect]->estimatedCardinality *= PREDICATE_SELECTIVITY;
    plan.appendOperator(move(filter));
}

void Enumerator::appendProjection(const vector<shared_ptr<Expression>>& expressions,
    LogicalPlan& plan, bool isRewritingAllProperties) {
    // Do not append projection in case of RETURN COUNT(*)
    if (1 == expressions.size() && COUNT_STAR_FUNC == expressions[0]->expressionType) {
        plan.isCountOnly = true;
        return;
    }
    auto expressionsToProject =
        rewriteExpressionToProject(expressions, graph.getCatalog(), isRewritingAllProperties);

    vector<uint32_t> exprResultInGroupPos;
    for (auto& expression : expressionsToProject) {
        auto unFlatGroupsPos = getUnFlatGroupsPos(*expression, *plan.schema);
        uint32_t groupPosToWrite = unFlatGroupsPos.empty() ?
                                       getAnyGroupPos(*expression, *plan.schema) :
                                       appendFlattensIfNecessary(unFlatGroupsPos, plan);
        exprResultInGroupPos.push_back(groupPosToWrite);
    }

    // reconstruct schema
    auto newSchema = make_unique<Schema>();
    newSchema->queryRelLogicalExtendMap = plan.schema->queryRelLogicalExtendMap;
    vector<uint32_t> exprResultOutGroupPos;
    exprResultOutGroupPos.resize(expressionsToProject.size());
    unordered_map<uint32_t, uint32_t> inToOutGroupPosMap;
    for (auto i = 0u; i < expressionsToProject.size(); ++i) {
        if (inToOutGroupPosMap.contains(exprResultInGroupPos[i])) {
            auto groupPos = inToOutGroupPosMap.at(exprResultInGroupPos[i]);
            exprResultOutGroupPos[i] = groupPos;
            newSchema->appendToGroup(expressionsToProject[i]->getInternalName(), groupPos);
        } else {
            auto groupPos = newSchema->createGroup();
            exprResultOutGroupPos[i] = groupPos;
            inToOutGroupPosMap.insert({exprResultInGroupPos[i], groupPos});
            newSchema->appendToGroup(expressionsToProject[i]->getInternalName(), groupPos);
            // copy other information
            newSchema->groups[groupPos]->isFlat =
                plan.schema->groups[exprResultInGroupPos[i]]->isFlat;
            newSchema->groups[groupPos]->estimatedCardinality =
                plan.schema->groups[exprResultInGroupPos[i]]->estimatedCardinality;
        }
    }

    // We collect the discarded group positions in the input data chunks to obtain their
    // multiplicity in the projection operation.
    vector<uint32_t> discardedGroupPos;
    for (auto i = 0u; i < plan.schema->groups.size(); ++i) {
        if (!inToOutGroupPosMap.contains(i)) {
            discardedGroupPos.push_back(i);
        }
    }

    auto projection = make_shared<LogicalProjection>(move(expressionsToProject), move(plan.schema),
        move(exprResultOutGroupPos), move(discardedGroupPos), plan.lastOperator);
    plan.schema = move(newSchema);
    plan.appendOperator(move(projection));
}

void Enumerator::appendScanPropertiesIfNecessary(Expression& expression, LogicalPlan& plan) {
    for (auto& expr : expression.getDependentProperties()) {
        auto& propertyExpression = (PropertyExpression&)*expr;
        if (plan.schema->containVariable(propertyExpression.getInternalName())) {
            continue;
        }
        NODE == propertyExpression.children[0]->dataType ?
            appendScanNodeProperty(propertyExpression, plan) :
            appendScanRelProperty(propertyExpression, plan);
    }
}

void Enumerator::appendScanNodeProperty(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto& nodeExpression = (const NodeExpression&)*propertyExpression.children[0];
    auto scanProperty = make_shared<LogicalScanNodeProperty>(nodeExpression.getIDProperty(),
        nodeExpression.label, propertyExpression.getInternalName(), propertyExpression.propertyKey,
        UNSTRUCTURED == propertyExpression.dataType, plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(nodeExpression.getIDProperty());
    plan.schema->appendToGroup(propertyExpression.getInternalName(), groupPos);
    plan.appendOperator(move(scanProperty));
}

void Enumerator::appendScanRelProperty(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto& relExpression = (const RelExpression&)*propertyExpression.children[0];
    auto extend = plan.schema->getExistingLogicalExtend(relExpression.getInternalName());
    auto scanProperty = make_shared<LogicalScanRelProperty>(extend->boundNodeID,
        extend->boundNodeLabel, extend->nbrNodeID, extend->relLabel, extend->direction,
        propertyExpression.getInternalName(), propertyExpression.propertyKey, extend->isColumn,
        plan.lastOperator);
    auto groupPos = plan.schema->getGroupPos(extend->nbrNodeID);
    plan.schema->appendToGroup(propertyExpression.getInternalName(), groupPos);
    plan.appendOperator(move(scanProperty));
}

void Enumerator::appendMultiplicityReducer(LogicalPlan& plan) {
    plan.appendOperator(make_shared<LogicalMultiplicityReducer>(plan.lastOperator));
}

void Enumerator::appendLimit(uint64_t limitNumber, LogicalPlan& plan) {
    auto unFlatGroupsPos = getUnFlatGroupsPos(*plan.schema);
    auto groupPosToSelect = unFlatGroupsPos.empty() ?
                                getAnyGroupPos(*plan.schema) :
                                appendFlattensIfNecessary(unFlatGroupsPos, plan);
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

void Enumerator::appendSkip(uint64_t skipNumber, LogicalPlan& plan) {
    auto unFlatGroupsPos = getUnFlatGroupsPos(*plan.schema);
    auto groupPosToSelect = unFlatGroupsPos.empty() ?
                                getAnyGroupPos(*plan.schema) :
                                appendFlattensIfNecessary(unFlatGroupsPos, plan);
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

unordered_set<uint32_t> getUnFlatGroupsPos(Expression& expression, const Schema& schema) {
    auto subExpressions = expression.getDependentLeafExpressions();
    unordered_set<uint32_t> unFlatGroupsPos;
    for (auto& subExpression : subExpressions) {
        if (schema.containVariable(subExpression->getInternalName())) {
            auto groupPos = schema.getGroupPos(subExpression->getInternalName());
            if (!schema.groups[groupPos]->isFlat) {
                unFlatGroupsPos.insert(groupPos);
            }
        } else {
            GF_ASSERT(ALIAS == subExpression->expressionType);
            for (auto& unFlatGroupPos : getUnFlatGroupsPos(*subExpression->children[0], schema)) {
                unFlatGroupsPos.insert(unFlatGroupPos);
            }
        }
    }
    return unFlatGroupsPos;
}

unordered_set<uint32_t> getUnFlatGroupsPos(const Schema& schema) {
    unordered_set<uint32_t> unFlatGroupsPos;
    for (auto i = 0u; i < schema.groups.size(); ++i) {
        if (!schema.groups[i]->isFlat) {
            unFlatGroupsPos.insert(i);
        }
    }
    return unFlatGroupsPos;
}

uint32_t getAnyGroupPos(Expression& expression, const Schema& schema) {
    auto subExpressions = expression.getDependentLeafExpressions();
    GF_ASSERT(!subExpressions.empty());
    return schema.getGroupPos(subExpressions[0]->getInternalName());
}

uint32_t getAnyGroupPos(const Schema& schema) {
    GF_ASSERT(!schema.groups.empty());
    return 0;
}

vector<shared_ptr<Expression>> rewriteExpressionToProject(
    vector<shared_ptr<Expression>> expressions, const Catalog& catalog,
    bool isRewritingAllProperties) {
    vector<shared_ptr<Expression>> result;
    for (auto& expression : expressions) {
        if (NODE == expression->dataType || REL == expression->dataType) {
            while (ALIAS == expression->expressionType) {
                expression = expression->children[0];
            }
            for (auto& propertyExpression :
                rewriteVariableExpression(expression, catalog, isRewritingAllProperties)) {
                result.push_back(propertyExpression);
            }
        } else {
            result.push_back(expression);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> rewriteVariableExpression(
    shared_ptr<Expression> expression, const Catalog& catalog, bool isRewritingAllProperties) {
    GF_ASSERT(VARIABLE == expression->expressionType);
    vector<shared_ptr<Expression>> result;
    if (NODE == expression->dataType) {
        auto nodeExpression = static_pointer_cast<NodeExpression>(expression);
        result.emplace_back(
            make_shared<PropertyExpression>(NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX, expression));
        if (isRewritingAllProperties) {
            for (auto& propertyExpression : createLogicalPropertyExpressions(
                     expression, catalog.getAllNodeProperties(nodeExpression->label))) {
                result.push_back(propertyExpression);
            }
        }
    } else {
        if (isRewritingAllProperties) {
            auto relExpression = static_pointer_cast<RelExpression>(expression);
            for (auto& propertyExpression : createLogicalPropertyExpressions(
                     expression, catalog.getRelProperties(relExpression->label))) {
                result.push_back(propertyExpression);
            }
        }
    }
    return result;
}

vector<shared_ptr<Expression>> createLogicalPropertyExpressions(
    shared_ptr<Expression> expression, const vector<PropertyDefinition>& properties) {
    vector<shared_ptr<Expression>> propertyExpressions;
    for (auto& property : properties) {
        propertyExpressions.emplace_back(make_shared<PropertyExpression>(
            property.dataType, property.name, property.id, expression));
    }
    return propertyExpressions;
}

} // namespace planner
} // namespace graphflow
