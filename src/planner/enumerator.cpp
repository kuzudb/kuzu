#include "src/planner/include/enumerator.h"

#include "src/binder/query/include/bound_regular_query.h"
#include "src/planner/logical_plan/logical_operator/include/logical_accumulate.h"
#include "src/planner/logical_plan/logical_operator/include/logical_copy_csv.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create_node_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create_rel_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_drop_table.h"
#include "src/planner/logical_plan/logical_operator/include/logical_expressions_scan.h"
#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_filter.h"
#include "src/planner/logical_plan/logical_operator/include/logical_flatten.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_property.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_rel_property.h"
#include "src/planner/logical_plan/logical_operator/include/logical_union.h"
#include "src/planner/logical_plan/logical_operator/include/logical_unwind.h"
#include "src/planner/logical_plan/logical_operator/include/sink_util.h"

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
    } else if (boundStatement.getStatementType() == StatementType::COPY_CSV) {
        resultPlans.push_back(createCopyCSVPlan((BoundCopyCSV&)boundStatement));
    } else if (boundStatement.getStatementType() == StatementType::DROP_TABLE) {
        resultPlans.push_back(createDropTablePlan((BoundDropTable&)boundStatement));
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
    } else if (boundStatement.getStatementType() == StatementType::COPY_CSV) {
        bestPlan = createCopyCSVPlan((BoundCopyCSV&)boundStatement);
    } else if (boundStatement.getStatementType() == StatementType::DROP_TABLE) {
        bestPlan = createDropTablePlan((BoundDropTable&)boundStatement);
    }
    return bestPlan;
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
    for (auto& plan : enumerateSingleQuery(singleQuery)) {
        // This is copy is to avoid sharing operator across plans. Later optimization requires
        // each plan to be independent.
        result.push_back(plan->deepCopy());
    }
    return result;
}

// Note: we cannot append ResultCollector for plans enumerated for single query before there could
// be a UNION on top which requires further flatten. So we delay ResultCollector appending to
// enumerate regular query level.
vector<unique_ptr<LogicalPlan>> Enumerator::enumerateSingleQuery(
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
    // TODO (Anurag): Supporting only 1 unwind query, nested unwind requires separate logical
    // operator
    if (queryPart.hasUnwindClause()) {
        assert(queryPart.getNumUnwindClause() == 1);
        for (auto& plan : plans) {
            planUnwindClause(*queryPart.getUnwindClause(0), *plan);
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

static expression_vector getCorrelatedExpressions(const QueryGraph& queryGraph,
    shared_ptr<Expression>& queryGraphPredicate, Schema* outerSchema) {
    expression_vector result;
    if (queryGraphPredicate) {
        result = outerSchema->getSubExpressionsInScope(queryGraphPredicate);
    }
    for (auto& nodeIDExpression : queryGraph.getNodeIDExpressions()) {
        if (outerSchema->isExpressionInScope(*nodeIDExpression)) {
            result.push_back(nodeIDExpression);
        }
    }
    return result;
}

static vector<shared_ptr<NodeExpression>> getJoinNodes(expression_vector& expressions) {
    vector<shared_ptr<NodeExpression>> joinNodes;
    for (auto& expression : expressions) {
        if (expression->dataType.typeID == NODE_ID) {
            auto node = static_pointer_cast<NodeExpression>(expression->getChild(0));
            joinNodes.push_back(std::move(node));
        }
    }
    return joinNodes;
}

void Enumerator::planUnwindClause(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(boundUnwindClause.getExpression(), groupPos);
    plan.appendOperator(make_shared<LogicalUnwind>(boundUnwindClause.getExpression()));
}

void Enumerator::planOptionalMatch(const QueryGraph& queryGraph,
    shared_ptr<Expression>& queryGraphPredicate, LogicalPlan& outerPlan) {
    auto correlatedExpressions =
        getCorrelatedExpressions(queryGraph, queryGraphPredicate, outerPlan.getSchema());
    if (ExpressionUtil::allExpressionsHaveDataType(correlatedExpressions, NODE_ID)) {
        auto joinNodes = getJoinNodes(correlatedExpressions);
        // When correlated variables are all NODE IDs, the subquery evaluation can be unnested as
        // left join (i.e. inner plan does not scan from outer plan). Join node is scanned in the
        // outer plan. Avoid scan the same table twice.
        auto innerQueryGraph = queryGraph.copyWithoutNodes(joinNodes);
        auto prevContext = joinOrderEnumerator.enterSubquery(&outerPlan, expression_vector{});
        auto innerPlans = joinOrderEnumerator.enumerateJoinOrder(
            *innerQueryGraph, queryGraphPredicate, getInitialEmptyPlans());
        auto bestInnerPlan = getBestPlan(std::move(innerPlans));
        joinOrderEnumerator.exitSubquery(std::move(prevContext));
        for (auto& joinNode : joinNodes) {
            appendFlattenIfNecessary(joinNode->getNodeIDPropertyExpression(), outerPlan);
        }
        JoinOrderEnumerator::planHashJoin(
            joinNodes, JoinType::LEFT, false /* isProbeAcc */, outerPlan, *bestInnerPlan);
    } else {
        // Note: the planning code for correlated optional match is ready to use. Once hash join
        // support any data type uncomment the code below.
        throw NotImplementedException("Correlated optional match is not supported.");
        //        appendAccumulate(outerPlan);
        //        auto prevContext = joinOrderEnumerator.enterSubquery(&outerPlan,
        //        correlatedExpressions); auto innerPlans = joinOrderEnumerator.enumerateJoinOrder(
        //            queryGraph, queryGraphPredicate, getInitialEmptyPlans());
        //        auto bestInnerPlan = getBestPlan(std::move(innerPlans));
        //        joinOrderEnumerator.exitSubquery(std::move(prevContext));
        //        for (auto& joinNode : joinNodes) {
        //            appendFlattenIfNecessary(joinNode->getNodeIDPropertyExpression(), outerPlan);
        //        }
        //        JoinOrderEnumerator::planHashJoin(
        //            joinNodes, JoinType::LEFT, true /* isProbeAcc */, outerPlan, *bestInnerPlan);
    }
}

void Enumerator::planExistsSubquery(shared_ptr<Expression>& expression, LogicalPlan& outerPlan) {
    assert(expression->expressionType == EXISTENTIAL_SUBQUERY);
    auto subquery = static_pointer_cast<ExistentialSubqueryExpression>(expression);
    auto correlatedExpressions = outerPlan.getSchema()->getSubExpressionsInScope(subquery);
    if (ExpressionUtil::allExpressionsHaveDataType(correlatedExpressions, NODE_ID)) {
        auto joinNodes = getJoinNodes(correlatedExpressions);
        // Unnest as mark join. See planOptionalMatch for unnesting logic.
        auto prevContext = joinOrderEnumerator.enterSubquery(&outerPlan, expression_vector{});
        auto queryGraphToEnumerate = subquery->getQueryGraph()->copyWithoutNodes(joinNodes);
        auto bestInnerPlan = getBestPlan(joinOrderEnumerator.enumerateJoinOrder(
            *queryGraphToEnumerate, subquery->getWhereExpression(), getInitialEmptyPlans()));
        joinOrderEnumerator.exitSubquery(std::move(prevContext));
        // TODO(Xiyang): add asp.
        JoinOrderEnumerator::appendMarkJoin(joinNodes, expression, outerPlan, *bestInnerPlan);
    } else {
        throw NotImplementedException("Correlated exists subquery is not supported.");
    }
}

void Enumerator::planSubqueryIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    if (expression->hasSubqueryExpression()) {
        for (auto& expr : expression->getTopLevelSubSubqueryExpressions()) {
            planExistsSubquery(expr, plan);
        }
    }
}

void Enumerator::appendAccumulate(graphflow::planner::LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto schemaBeforeSink = schema->copy();
    SinkOperatorUtil::reComputeSchema(*schemaBeforeSink, *schema);
    vector<uint64_t> flatOutputGroupPositions;
    for (auto i = 0u; i < schema->getNumGroups(); ++i) {
        if (schema->getGroup(i)->getIsFlat()) {
            flatOutputGroupPositions.push_back(i);
        }
    }
    auto sink = make_shared<LogicalAccumulate>(schemaBeforeSink->getExpressionsInScope(),
        flatOutputGroupPositions, std::move(schemaBeforeSink), plan.getLastOperator());
    plan.appendOperator(sink);
}

void Enumerator::appendExpressionsScan(expression_vector& expressions, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto schema = plan.getSchema();
    auto groupPos = schema->createGroup();
    schema->flattenGroup(groupPos); // Mark group holding constant as flat.
    for (auto& expression : expressions) {
        schema->insertToGroupAndScope(expression, groupPos);
    }
    auto expressionsScan = make_shared<LogicalExpressionsScan>(std::move(expressions));
    plan.appendOperator(std::move(expressionsScan));
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

void Enumerator::appendFlattenIfNecessary(
    const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto group = schema->getGroup(expression);
    if (group->getIsFlat()) {
        return;
    }
    auto flatten = make_shared<LogicalFlatten>(expression, plan.getLastOperator());
    flatten->computeSchema(*schema);
    plan.appendOperator(std::move(flatten));
    // update cardinality estimation info
    plan.multiplyCardinality(group->getMultiplier());
}

void Enumerator::appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan) {
    planSubqueryIfNecessary(expression, plan);
    auto dependentGroupsPos = plan.getSchema()->getDependentGroupsPos(expression);
    auto groupPosToSelect = appendFlattensButOne(dependentGroupsPos, plan);
    auto filter = make_shared<LogicalFilter>(expression, groupPosToSelect, plan.getLastOperator());
    plan.multiplyCardinality(EnumeratorKnobs::PREDICATE_SELECTIVITY);
    plan.appendOperator(std::move(filter));
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
        make_shared<LogicalScanNodeProperty>(node.getIDProperty(), node.getTableID(),
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
        catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(rel.getTableID(), direction);
    assert(expression->expressionType == PROPERTY);
    auto property = static_pointer_cast<PropertyExpression>(expression);
    auto scanProperty = make_shared<LogicalScanRelProperty>(boundNode, nbrNode, rel.getTableID(),
        direction, property->getUniqueName(), property->getPropertyKey(), isColumn,
        plan.getLastOperator());
    auto groupPos = schema->getGroupPos(nbrNode->getIDProperty());
    schema->insertToGroupAndScope(property, groupPos);
    plan.appendOperator(move(scanProperty));
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
    SinkOperatorUtil::reComputeSchema(*firstChildSchema, *plan->getSchema());
    vector<unique_ptr<Schema>> schemaBeforeUnion;
    vector<shared_ptr<LogicalOperator>> children;
    for (auto& childPlan : childrenPlans) {
        plan->increaseCost(childPlan->getCost());
        schemaBeforeUnion.push_back(childPlan->getSchema()->copy());
        children.push_back(childPlan->getLastOperator());
    }
    auto logicalUnion = make_shared<LogicalUnion>(
        firstChildSchema->getExpressionsInScope(), move(schemaBeforeUnion), move(children));
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
    const BoundCreateNodeClause& boundCreateNodeClause) {
    auto plan = make_unique<LogicalPlan>();
    plan->appendOperator(make_shared<LogicalCreateNodeTable>(boundCreateNodeClause.getTableName(),
        boundCreateNodeClause.getPropertyNameDataTypes(),
        boundCreateNodeClause.getPrimaryKeyIdx()));
    return plan;
}

unique_ptr<LogicalPlan> Enumerator::createCreateRelTablePlan(
    const BoundCreateRelClause& boundCreateRelClause) {
    auto plan = make_unique<LogicalPlan>();
    plan->appendOperator(make_shared<LogicalCreateRelTable>(boundCreateRelClause.getTableName(),
        boundCreateRelClause.getPropertyNameDataTypes(), boundCreateRelClause.getRelMultiplicity(),
        boundCreateRelClause.getSrcDstTableIDs()));
    return plan;
}

unique_ptr<LogicalPlan> Enumerator::createCopyCSVPlan(const BoundCopyCSV& boundCopyCSV) {
    auto plan = make_unique<LogicalPlan>();
    plan->appendOperator(make_shared<LogicalCopyCSV>(
        boundCopyCSV.getCSVDescription(), boundCopyCSV.getTableSchema()));
    return plan;
}

unique_ptr<LogicalPlan> Enumerator::createDropTablePlan(const BoundDropTable& boundDropTable) {
    auto plan = make_unique<LogicalPlan>();
    plan->appendOperator(make_shared<LogicalDropTable>(boundDropTable.getTableSchema()));
    return plan;
}

} // namespace planner
} // namespace graphflow
