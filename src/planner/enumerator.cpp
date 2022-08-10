#include "src/planner/include/enumerator.h"

#include "src/binder/query/include/bound_regular_query.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create_node_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create_rel_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_exist.h"
#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_filter.h"
#include "src/planner/logical_plan/logical_operator/include/logical_flatten.h"
#include "src/planner/logical_plan/logical_operator/include/logical_left_nested_loop_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_property.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_rel_property.h"
#include "src/planner/logical_plan/logical_operator/include/logical_sink.h"
#include "src/planner/logical_plan/logical_operator/include/logical_union.h"

namespace graphflow {
namespace planner {

vector<unique_ptr<LogicalPlan>> Enumerator::getAllPlans(const BoundStatement& boundStatement) {
    vector<unique_ptr<LogicalPlan>> resultPlans;
    if (boundStatement.getStatementType() == StatementType::QUERY) {
        auto& regularQuery = (BoundRegularQuery&)boundStatement;
        if (regularQuery.getNumSingleQueries() == 1) {
            resultPlans = getAllPlans(*regularQuery.getSingleQuery(0));
        } else {
            vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans(
                regularQuery.getNumSingleQueries());
            for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
                childrenLogicalPlans[i] = getAllPlans(*regularQuery.getSingleQuery(i));
            }
            auto childrenPlans = cartesianProductChildrenPlans(move(childrenLogicalPlans));
            for (auto& childrenPlan : childrenPlans) {
                resultPlans.push_back(createUnionPlan(childrenPlan, regularQuery.getIsUnionAll(0)));
            }
        }
        for (auto& plan : resultPlans) {
            plan->setExpressionsToCollect(regularQuery.getExpressionsToReturn());
        }
    } else if (boundStatement.getStatementType() == StatementType::CREATE_NODE_CLAUSE) {
        resultPlans.push_back(createCreateNodeTablePlan((BoundCreateNodeClause&)boundStatement));
    } else if (boundStatement.getStatementType() == StatementType::CREATE_REL_CLAUSE) {
        resultPlans.push_back(createCreateRelTablePlan((BoundCreateRelClause&)boundStatement));
    }
    return resultPlans;
}

unique_ptr<LogicalPlan> Enumerator::getBestPlan(const BoundStatement& boundStatement) {
    unique_ptr<LogicalPlan> bestPlan;
    if (boundStatement.getStatementType() == StatementType::QUERY) {
        auto& regularQuery = (BoundRegularQuery&)boundStatement;
        if (regularQuery.getNumSingleQueries() == 1) {
            bestPlan = getBestPlan(*regularQuery.getSingleQuery(0));
        } else {
            vector<unique_ptr<LogicalPlan>> childrenPlans(regularQuery.getNumSingleQueries());
            for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
                childrenPlans[i] = getBestPlan(*regularQuery.getSingleQuery(i));
            }
            bestPlan = createUnionPlan(childrenPlans, regularQuery.getIsUnionAll(0));
        }
        bestPlan->setExpressionsToCollect(regularQuery.getExpressionsToReturn());
    } else if (boundStatement.getStatementType() == StatementType::CREATE_NODE_CLAUSE) {
        bestPlan = createCreateNodeTablePlan((BoundCreateNodeClause&)boundStatement);
    } else if (boundStatement.getStatementType() == StatementType::CREATE_REL_CLAUSE) {
        bestPlan = createCreateRelTablePlan((BoundCreateRelClause&)boundStatement);
    }
    return bestPlan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::getValidSubPlans(
    vector<unique_ptr<LogicalPlan>> plans) {
    vector<unique_ptr<LogicalPlan>> results;
    for (auto& plan : plans) {
        if (plan->containOperatorsNotAllowedInSubPlan()) {
            continue;
        }
        results.push_back(move(plan));
    }
    return results;
}

unique_ptr<LogicalPlan> Enumerator::getBestPlan(vector<unique_ptr<LogicalPlan>> plans) {
    auto bestPlan = move(plans[0]);
    for (auto i = 1u; i < plans.size(); ++i) {
        if (plans[i]->getCost() < bestPlan->getCost()) {
            bestPlan = move(plans[i]);
        }
    }
    return bestPlan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::getAllPlans(const NormalizedSingleQuery& singleQuery) {
    vector<unique_ptr<LogicalPlan>> result;
    for (auto& plan : enumeratePlans(singleQuery)) {
        // This is copy is to avoid sharing operator across plans. Later optimization requires
        // each plan to be independent.
        result.push_back(plan->deepCopy());
    }
    return result;
}

// Note: we cannot append ResultCollector for plans enumerated for single query before there could
// be a UNION on top which requires further flatten. So we delay ResultCollector appending to
// enumerate regular query level.
vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans(
    const NormalizedSingleQuery& singleQuery) {
    propertiesToScan.clear();
    for (auto& expression : singleQuery.getPropertiesToRead()) {
        assert(expression->expressionType == PROPERTY);
        propertiesToScan.push_back(expression);
    }
    joinOrderEnumerator.resetState();
    auto plans = getInitialEmptyPlans();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        plans = enumerateQueryPart(*singleQuery.getQueryPart(i), move(plans));
    }
    return plans;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumerateQueryPart(
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    vector<unique_ptr<LogicalPlan>> plans = move(prevPlans);
    // plan read
    for (auto i = 0u; i < queryPart.getNumQueryGraph(); ++i) {
        if (queryPart.isQueryGraphOptional(i)) {
            auto& queryGraph = *queryPart.getQueryGraph(i);
            auto queryGraphPredicate = queryPart.getQueryGraphPredicate(i);
            for (auto& plan : plans) {
                planOptionalMatch(queryGraph, queryGraphPredicate, *plan);
            }
            // Although optional match is planned as a subquery, we still need to merge query graph
            // as we merge sub query schema into outer query.
            joinOrderEnumerator.context->mergeQueryGraph(queryGraph);
        } else {
            plans = joinOrderEnumerator.enumerateJoinOrder(
                *queryPart.getQueryGraph(i), queryPart.getQueryGraphPredicate(i), move(plans));
        }
    }
    // plan update
    for (auto i = 0u; i < queryPart.getNumUpdatingClause(); ++i) {
        updatePlanner.planUpdatingClause(*queryPart.getUpdatingClause(i), plans);
    }
    if (queryPart.hasProjectionBody()) {
        projectionEnumerator.enumerateProjectionBody(*queryPart.getProjectionBody(), plans);
        if (queryPart.hasProjectionBodyPredicate()) {
            for (auto& plan : plans) {
                appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
            }
        }
    }
    return plans;
}

void Enumerator::planOptionalMatch(const QueryGraph& queryGraph,
    const shared_ptr<Expression>& queryGraphPredicate, LogicalPlan& outerPlan) {
    auto outerPlanSchema = outerPlan.getSchema();
    expression_vector expressionsToScanFromOuter;
    if (queryGraphPredicate) {
        expressionsToScanFromOuter =
            getSubExpressionsInSchema(queryGraphPredicate, *outerPlanSchema);
    }
    for (auto& nodeIDExpression : queryGraph.getNodeIDExpressions()) {
        if (outerPlanSchema->isExpressionInScope(*nodeIDExpression)) {
            expressionsToScanFromOuter.push_back(nodeIDExpression);
        }
    }
    for (auto& expression : expressionsToScanFromOuter) {
        appendFlattenIfNecessary(
            outerPlanSchema->getGroupPos(expression->getUniqueName()), outerPlan);
    }
    auto prevContext = joinOrderEnumerator.enterSubquery(move(expressionsToScanFromOuter));
    auto bestPlan = getBestPlan(getValidSubPlans(joinOrderEnumerator.enumerateJoinOrder(
        queryGraph, queryGraphPredicate, getInitialEmptyPlans())));
    joinOrderEnumerator.exitSubquery(move(prevContext));
    // Schema merging logic for optional match:
    //    Consider example MATCH (a:Person) OPTIONAL MATCH (a)-[:studyAt]->(b:Organisation)
    //    where (a)-[:studyAt]->(b:Organisation) is a column extend. The inner resultSet
    //    contains 1 dataChunk DCInner with 2 vectors. In the outer query, we shallow copy the
    //    vector b from the inner query. However we put b's vector not to a's datachunk DCOuterA but
    //    to a different datachunk DCOuterB. This is because the b's vector contains DCInner's
    //    state, which might be different than DCOuterA. To make sure that when vector b is copied
    //    to the outer query, we can keep DCInner's state, we copy B to a new datachunk.
    auto subPlanSchema = bestPlan->getSchema()->copy();
    assert(subPlanSchema->getNumGroups() > 0);
    auto firstInnerGroup = subPlanSchema->getGroup(0);
    auto firstInnerGroupMapToOuterPos = UINT32_MAX;
    // Merge first inner group into outer. This merging handles the logic above.
    for (auto& expression : firstInnerGroup->getExpressions()) {
        if (outerPlanSchema->isExpressionInScope(*expression)) {
            continue;
        }
        if (firstInnerGroupMapToOuterPos == UINT32_MAX) {
            firstInnerGroupMapToOuterPos = outerPlanSchema->createGroup();
        }
        outerPlanSchema->insertToGroupAndScope(expression, firstInnerGroupMapToOuterPos);
        outerPlanSchema->getGroup(firstInnerGroupMapToOuterPos)
            ->setIsFlat(firstInnerGroup->getIsFlat());
    }
    // Merge rest inner groups into outer.
    for (auto i = 1u; i < subPlanSchema->getNumGroups(); ++i) {
        auto outerPos = outerPlanSchema->createGroup();
        auto group = subPlanSchema->getGroup(i);
        for (auto& expression : group->getExpressions()) {
            outerPlanSchema->insertToGroupAndScope(expression, outerPos);
        }
        outerPlanSchema->getGroup(outerPos)->setIsFlat(group->getIsFlat());
    }
    auto logicalLeftNestedLoopJoin = make_shared<LogicalLeftNestedLoopJoin>(
        bestPlan->getSchema()->copy(), outerPlan.getLastOperator(), bestPlan->getLastOperator());
    outerPlan.appendOperator(move(logicalLeftNestedLoopJoin));
}

void Enumerator::planExistsSubquery(
    const shared_ptr<ExistentialSubqueryExpression>& subqueryExpression, LogicalPlan& outerPlan) {
    auto expressionsToScanFromOuter =
        getSubExpressionsInSchema(subqueryExpression, *outerPlan.getSchema());
    // We flatten all dependent groups for subquery and the result of subquery evaluation can be
    // appended to any flat dependent group.
    auto groupPosToWrite = UINT32_MAX;
    for (auto& expression : expressionsToScanFromOuter) {
        groupPosToWrite = outerPlan.getSchema()->getGroupPos(expression->getUniqueName());
        appendFlattenIfNecessary(groupPosToWrite, outerPlan);
    }
    outerPlan.getSchema()->insertToGroupAndScope(subqueryExpression, groupPosToWrite);
    auto& normalizedQuery = *subqueryExpression->getSubquery();
    auto prevContext = joinOrderEnumerator.enterSubquery(move(expressionsToScanFromOuter));
    auto plans = enumerateQueryPart(*normalizedQuery.getQueryPart(0), getInitialEmptyPlans());
    joinOrderEnumerator.context->clearExpressionsToScanFromOuter();
    for (auto i = 1u; i < normalizedQuery.getNumQueryParts(); ++i) {
        plans = enumerateQueryPart(*normalizedQuery.getQueryPart(i), move(plans));
    }
    joinOrderEnumerator.exitSubquery(move(prevContext));
    auto bestPlan = getBestPlan(getValidSubPlans(move(plans)));
    auto logicalExists = make_shared<LogicalExists>(subqueryExpression,
        bestPlan->getSchema()->copy(), outerPlan.getLastOperator(), bestPlan->getLastOperator());
    outerPlan.appendOperator(logicalExists);
}

void Enumerator::planSubqueryIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    if (expression->hasSubqueryExpression()) {
        for (auto& expr : expression->getTopLevelSubSubqueryExpressions()) {
            auto subqueryExpression = static_pointer_cast<ExistentialSubqueryExpression>(expr);
            planExistsSubquery(subqueryExpression, plan);
        }
    }
}

void Enumerator::appendFlattens(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan) {
    for (auto& groupPos : groupsPos) {
        appendFlattenIfNecessary(groupPos, plan);
    }
}

uint32_t Enumerator::appendFlattensButOne(
    const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan) {
    if (groupsPos.empty()) {
        // an expression may not depend on any group. E.g. COUNT(*).
        return UINT32_MAX;
    }
    vector<uint32_t> unFlatGroupsPos;
    for (auto& groupPos : groupsPos) {
        if (!plan.getSchema()->getGroup(groupPos)->getIsFlat()) {
            unFlatGroupsPos.push_back(groupPos);
        }
    }
    if (unFlatGroupsPos.empty()) {
        return *groupsPos.begin();
    }
    for (auto i = 1u; i < unFlatGroupsPos.size(); ++i) {
        appendFlattenIfNecessary(unFlatGroupsPos[i], plan);
    }
    return unFlatGroupsPos[0];
}

void Enumerator::appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan) {
    auto& group = *plan.getSchema()->getGroup(groupPos);
    if (group.getIsFlat()) {
        return;
    }
    plan.getSchema()->flattenGroup(groupPos);
    plan.multiplyCardinality(group.getMultiplier());
    auto flatten = make_shared<LogicalFlatten>(group.getAnyExpression(), plan.getLastOperator());
    plan.appendOperator(move(flatten));
}

void Enumerator::appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    planSubqueryIfNecessary(expression, plan);
    auto dependentGroupsPos = getDependentGroupsPos(expression, *plan.getSchema());
    auto groupPosToSelect = appendFlattensButOne(dependentGroupsPos, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.getLastOperator());
    plan.multiplyCardinality(EnumeratorKnobs::PREDICATE_SELECTIVITY);
    plan.appendOperator(move(filter));
}

void Enumerator::appendScanNodePropIfNecessarySwitch(
    expression_vector& properties, NodeExpression& node, LogicalPlan& plan) {
    expression_vector structuredProperties;
    expression_vector unstructuredProperties;
    for (auto& property : properties) {
        if (property->dataType.typeID == UNSTRUCTURED) {
            unstructuredProperties.push_back(property);
        } else {
            structuredProperties.push_back(property);
        }
    }
    appendScanNodePropIfNecessary(structuredProperties, node, plan, true /* isStructured */);
    appendScanNodePropIfNecessary(unstructuredProperties, node, plan, false /* isUnstructured */);
}

void Enumerator::appendScanNodePropIfNecessary(
    expression_vector& properties, NodeExpression& node, LogicalPlan& plan, bool isStructured) {
    auto schema = plan.getSchema();
    vector<string> propertyNames;
    vector<uint32_t> propertyKeys;
    auto groupPos = schema->getGroupPos(node.getIDProperty());
    for (auto& expression : properties) {
        if (schema->isExpressionInScope(*expression)) {
            continue;
        }
        assert(expression->expressionType == PROPERTY);
        auto property = static_pointer_cast<PropertyExpression>(expression);
        propertyNames.push_back(property->getUniqueName());
        propertyKeys.push_back(property->getPropertyKey());
        schema->insertToGroupAndScope(property, groupPos);
    }
    if (propertyNames.empty()) { // all properties have been scanned before
        return;
    }
    auto scanNodeProperty =
        make_shared<LogicalScanNodeProperty>(node.getIDProperty(), node.getLabel(),
            move(propertyNames), move(propertyKeys), !isStructured, plan.getLastOperator());
    plan.appendOperator(move(scanNodeProperty));
}

void Enumerator::appendScanRelPropIfNecessary(shared_ptr<Expression>& expression,
    RelExpression& rel, RelDirection direction, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    if (schema->isExpressionInScope(*expression)) {
        return;
    }
    auto boundNode = FWD == direction ? rel.getSrcNode() : rel.getDstNode();
    auto nbrNode = FWD == direction ? rel.getDstNode() : rel.getSrcNode();
    auto isColumn =
        catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(rel.getLabel(), direction);
    assert(expression->expressionType == PROPERTY);
    auto property = static_pointer_cast<PropertyExpression>(expression);
    auto scanProperty = make_shared<LogicalScanRelProperty>(boundNode, nbrNode, rel.getLabel(),
        direction, property->getUniqueName(), property->getPropertyKey(), isColumn,
        plan.getLastOperator());
    auto groupPos = schema->getGroupPos(nbrNode->getIDProperty());
    schema->insertToGroupAndScope(property, groupPos);
    plan.appendOperator(move(scanProperty));
}

void Enumerator::appendSink(LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto schemaBeforeSink = schema->copy();
    schema->clear();
    Enumerator::computeSchemaForSinkOperators(
        schemaBeforeSink->getGroupsPosInScope(), *schemaBeforeSink, *schema);
    vector<uint64_t> flatOutputGroupPositions;
    for (auto i = 0u; i < schema->getNumGroups(); ++i) {
        if (schema->getGroup(i)->getIsFlat()) {
            flatOutputGroupPositions.push_back(i);
        }
    }
    auto sink = make_shared<LogicalSink>(schemaBeforeSink->getExpressionsInScope(),
        flatOutputGroupPositions, move(schemaBeforeSink), plan.getLastOperator());
    plan.appendOperator(sink);
}

unique_ptr<LogicalPlan> Enumerator::createUnionPlan(
    vector<unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll) {
    // If an expression to union has different flat/unflat state in different child, we
    // need to flatten that expression in all the single queries.
    assert(!childrenPlans.empty());
    auto firstChildSchema = childrenPlans[0]->getSchema();
    auto numExpressionsToUnion = firstChildSchema->getExpressionsInScope().size();
    for (auto i = 0u; i < numExpressionsToUnion; i++) {
        bool hasFlatExpression = false;
        for (auto& childPlan : childrenPlans) {
            auto childSchema = childPlan->getSchema();
            auto expressionName = childSchema->getExpressionsInScope()[i]->getUniqueName();
            hasFlatExpression |= childSchema->getGroup(expressionName)->getIsFlat();
        }
        if (hasFlatExpression) {
            for (auto& childPlan : childrenPlans) {
                auto childSchema = childPlan->getSchema();
                auto expressionName = childSchema->getExpressionsInScope()[i]->getUniqueName();
                appendFlattenIfNecessary(childSchema->getGroupPos(expressionName), *childPlan);
            }
        }
    }
    // we compute the schema based on first child
    auto plan = make_unique<LogicalPlan>();
    auto logicalUnion = make_shared<LogicalUnion>(firstChildSchema->getExpressionsInScope());
    Enumerator::computeSchemaForSinkOperators(
        firstChildSchema->getGroupsPosInScope(), *firstChildSchema, *plan->getSchema());
    for (auto& childPlan : childrenPlans) {
        plan->increaseCost(childPlan->getCost());
        logicalUnion->addChild(childPlan->getLastOperator());
        logicalUnion->addSchema(childPlan->getSchema()->copy());
    }
    plan->appendOperator(logicalUnion);
    if (!isUnionAll) {
        projectionEnumerator.appendDistinct(logicalUnion->getExpressionsToUnion(), *plan);
    }
    return plan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::getInitialEmptyPlans() {
    vector<unique_ptr<LogicalPlan>> plans;
    plans.push_back(make_unique<LogicalPlan>());
    return plans;
}

expression_vector Enumerator::getPropertiesForNode(NodeExpression& node) {
    expression_vector result;
    for (auto& property : propertiesToScan) {
        if (property->getChild(0)->getUniqueName() == node.getUniqueName()) {
            assert(property->getChild(0)->dataType.typeID == NODE);
            result.push_back(property);
        }
    }
    return result;
}

expression_vector Enumerator::getPropertiesForRel(RelExpression& rel) {
    expression_vector result;
    for (auto& property : propertiesToScan) {
        if (property->getChild(0)->getUniqueName() == rel.getUniqueName()) {
            assert(property->getChild(0)->dataType.typeID == REL);
            result.push_back(property);
        }
    }
    return result;
}

unordered_set<uint32_t> Enumerator::getDependentGroupsPos(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    unordered_set<uint32_t> dependentGroupsPos;
    for (auto& subExpression : getSubExpressionsInSchema(expression, schema)) {
        dependentGroupsPos.insert(schema.getGroupPos(subExpression->getUniqueName()));
    }
    return dependentGroupsPos;
}

expression_vector Enumerator::getSubExpressionsInSchema(
    const shared_ptr<Expression>& expression, const Schema& schema) {
    expression_vector results;
    if (schema.isExpressionInScope(*expression)) {
        results.push_back(expression);
        return results;
    }
    for (auto& child : expression->getChildren()) {
        for (auto& subExpression : getSubExpressionsInSchema(child, schema)) {
            results.push_back(subExpression);
        }
    }
    return results;
}

void Enumerator::computeSchemaForSinkOperators(
    const unordered_set<uint32_t>& groupsToMaterializePos, const Schema& schemaBeforeSink,
    Schema& schemaAfterSink) {
    auto flatVectorsOutputPos = UINT32_MAX;
    auto isAllVectorsFlat = true;
    for (auto& groupToMaterializePos : groupsToMaterializePos) {
        auto groupToMaterialize = schemaBeforeSink.getGroup(groupToMaterializePos);
        if (groupToMaterialize->getIsFlat()) {
            if (flatVectorsOutputPos == UINT32_MAX) {
                flatVectorsOutputPos = schemaAfterSink.createGroup();
            }
            schemaAfterSink.insertToGroupAndScope(
                schemaBeforeSink.getExpressionsInScope(groupToMaterializePos),
                flatVectorsOutputPos);
        } else {
            isAllVectorsFlat = false;
            auto groupPos = schemaAfterSink.createGroup();
            auto group = schemaAfterSink.getGroup(groupPos);
            schemaAfterSink.insertToGroupAndScope(
                schemaBeforeSink.getExpressionsInScope(groupToMaterializePos), groupPos);
            group->setMultiplier(groupToMaterialize->getMultiplier());
        }
    }
    if (!isAllVectorsFlat && flatVectorsOutputPos != UINT32_MAX) {
        schemaAfterSink.flattenGroup(flatVectorsOutputPos);
    }
}

vector<vector<unique_ptr<LogicalPlan>>> Enumerator::cartesianProductChildrenPlans(
    vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans) {
    vector<vector<unique_ptr<LogicalPlan>>> resultChildrenPlans;
    for (auto& childLogicalPlans : childrenLogicalPlans) {
        vector<vector<unique_ptr<LogicalPlan>>> curChildResultLogicalPlans;
        for (auto& childLogicalPlan : childLogicalPlans) {
            if (resultChildrenPlans.empty()) {
                vector<unique_ptr<LogicalPlan>> logicalPlans;
                logicalPlans.push_back(childLogicalPlan->deepCopy());
                curChildResultLogicalPlans.push_back(move(logicalPlans));
            } else {
                for (auto& resultChildPlans : resultChildrenPlans) {
                    vector<unique_ptr<LogicalPlan>> logicalPlans;
                    for (auto& resultChildPlan : resultChildPlans) {
                        logicalPlans.push_back(resultChildPlan->deepCopy());
                    }
                    logicalPlans.push_back(childLogicalPlan->deepCopy());
                    curChildResultLogicalPlans.push_back(move(logicalPlans));
                }
            }
        }
        resultChildrenPlans = move(curChildResultLogicalPlans);
    }
    return resultChildrenPlans;
}

unique_ptr<LogicalPlan> Enumerator::createCreateNodeTablePlan(
    BoundCreateNodeClause& boundCreateNodeClause) {
    auto plan = make_unique<LogicalPlan>();
    plan->appendOperator(make_shared<LogicalCreateNodeTable>(boundCreateNodeClause.getLabelName(),
        boundCreateNodeClause.getPropertyNameDataTypes(),
        boundCreateNodeClause.getPrimaryKeyIdx()));
    return plan;
}

unique_ptr<LogicalPlan> Enumerator::createCreateRelTablePlan(
    BoundCreateRelClause& boundCreateRelClause) {
    auto plan = make_unique<LogicalPlan>();
    plan->appendOperator(make_shared<LogicalCreateRelTable>(boundCreateRelClause.getLabelName(),
        boundCreateRelClause.getPropertyNameDataTypes(), boundCreateRelClause.getRelMultiplicity(),
        boundCreateRelClause.getSrcDstLabels()));
    return plan;
}

} // namespace planner
} // namespace graphflow
