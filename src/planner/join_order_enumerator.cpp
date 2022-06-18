#include "src/planner/include/join_order_enumerator.h"

#include "src/binder/expression/include/function_expression.h"
#include "src/planner/include/enumerator.h"
#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/planner/logical_plan/logical_operator/include/logical_result_scan.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"

namespace graphflow {
namespace planner {

static expression_vector getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const expression_vector& expressions);
static expression_vector getNewMatchedExpressions(const SubqueryGraph& prevLeftSubgraph,
    const SubqueryGraph& prevRightSubgraph, const SubqueryGraph& newSubgraph,
    const expression_vector& expressions);
static shared_ptr<Expression> createNodeIDComparison(const shared_ptr<Expression>& left,
    const shared_ptr<Expression>& right, BuiltInVectorOperations* builtInFunctions);

// Rewrite a query rel that closes a cycle as a regular query rel for extend. This requires giving a
// different identifier to the node that will close the cycle. This identifier is created as rel
// name + node name.
static shared_ptr<RelExpression> rewriteQueryRel(const RelExpression& queryRel, bool isRewriteDst);

vector<unique_ptr<LogicalPlan>> JoinOrderEnumerator::enumerateJoinOrder(
    const QueryGraph& queryGraph, const shared_ptr<Expression>& queryGraphPredicate,
    vector<unique_ptr<LogicalPlan>> prevPlans) {
    context->init(queryGraph, queryGraphPredicate, move(prevPlans));
    context->hasExpressionsToScanFromOuter() ? planResultScan() : planNodeScan();
    while (context->hasNextLevel()) {
        context->incrementCurrentLevel();
        planExtend();
        // TODO: remove currentLevel constraint for Hash Join enumeration
        if (context->getCurrentLevel() >= 4) {
            planHashJoin();
        }
    }
    return move(context->getPlans(context->getFullyMatchedSubqueryGraph()));
}

unique_ptr<JoinOrderEnumeratorContext> JoinOrderEnumerator::enterSubquery(
    expression_vector expressionsToScan) {
    auto prevContext = move(context);
    context = make_unique<JoinOrderEnumeratorContext>();
    context->setExpressionsToScanFromOuter(move(expressionsToScan));
    return prevContext;
}

void JoinOrderEnumerator::exitSubquery(unique_ptr<JoinOrderEnumeratorContext> prevContext) {
    context = move(prevContext);
}

void JoinOrderEnumerator::planResultScan() {
    auto emptySubgraph = context->getEmptySubqueryGraph();
    auto plan = context->containPlans(emptySubgraph) ?
                    context->getPlans(emptySubgraph)[0]->shallowCopy() :
                    make_unique<LogicalPlan>();
    auto newSubgraph = emptySubgraph;
    for (auto& expression : context->getExpressionsToScanFromOuter()) {
        if (expression->dataType.typeID == NODE_ID) {
            assert(expression->expressionType == PROPERTY);
            newSubgraph.addQueryNode(context->getQueryGraph()->getQueryNodePos(
                expression->getChild(0)->getUniqueName()));
        }
    }
    appendResultScan(context->getExpressionsToScanFromOuter(), *plan);
    for (auto& expression :
        getNewMatchedExpressions(emptySubgraph, newSubgraph, context->getWhereExpressions())) {
        enumerator->appendFilter(expression, *plan);
    }
    context->addPlan(newSubgraph, move(plan));
}

void JoinOrderEnumerator::planNodeScan() {
    auto emptySubgraph = context->getEmptySubqueryGraph();
    if (context->getMatchedQueryNodes().count() == 1) {
        // If only single node has been previously enumerated, then join order is decided
        return;
    }
    auto queryGraph = context->getQueryGraph();
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes(); ++nodePos) {
        auto newSubgraph = context->getEmptySubqueryGraph();
        newSubgraph.addQueryNode(nodePos);
        auto plan = make_unique<LogicalPlan>();
        auto& node = *queryGraph->getQueryNode(nodePos);
        appendScanNodeID(node, *plan);
        auto predicates =
            getNewMatchedExpressions(emptySubgraph, newSubgraph, context->getWhereExpressions());
        planFiltersAfterNodeScan(predicates, node, *plan);
        planPropertyScansAfterNodeScan(node, *plan);
        context->addPlan(newSubgraph, move(plan));
    }
}

void JoinOrderEnumerator::planFiltersAfterNodeScan(
    expression_vector& predicates, NodeExpression& node, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto propertiesToScan = getPropertiesForVariable(*predicate, node);
        enumerator->appendScanNodePropIfNecessarySwitch(propertiesToScan, node, plan);
        enumerator->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansAfterNodeScan(NodeExpression& node, LogicalPlan& plan) {
    auto properties = enumerator->getPropertiesForNode(node);
    enumerator->appendScanNodePropIfNecessarySwitch(properties, node, plan);
}

void JoinOrderEnumerator::planExtend() {
    auto queryGraph = context->getQueryGraph();
    auto& subgraphPlansMap = context->getSubqueryGraphPlansMap(context->getCurrentLevel() - 1);
    for (auto& [prevSubgraph, prevPlans] : subgraphPlansMap) {
        auto connectedQueryRelsWithDirection =
            queryGraph->getConnectedQueryRelsWithDirection(prevSubgraph);
        for (auto& [relPos, isSrcMatched, isDstMatched] : connectedQueryRelsWithDirection) {
            // Consider query MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) WITH *
            // MATCH (d)->[r4]->(e)-[r5]->(f) RETURN *
            // First MATCH is enumerated normally. When enumerating second MATCH,
            // we first merge graph as (a)-[r1]->(b)-[r2]->(c)-[r3]->(d)->[r4]->(e)-[r5]->(f) and
            // enumerate from level 0 again. If we hit a query rel that has been previously matched
            // i.e. r1 & r2 & r3, we skip the plan. This guarantees DP only enumerate query rels in
            // the second MATCH. Note this is different from fully merged, since we don't generate
            // plans like build side QVO : a, b, c,  probe side QVO: f, e, d, c, HashJoin(c).
            if (context->getMatchedQueryRels()[relPos]) {
                continue;
            }
            auto newSubgraph = prevSubgraph;
            newSubgraph.addQueryRel(relPos);
            auto predicates =
                getNewMatchedExpressions(prevSubgraph, newSubgraph, context->getWhereExpressions());
            auto& queryRel = *queryGraph->getQueryRel(relPos);
            if (isSrcMatched && isDstMatched) {
                // TODO: refactor cyclic logic as a separate function
                for (auto& prevPlan : prevPlans) {
                    for (auto direction : REL_DIRECTIONS) {
                        auto isCloseOnDst = direction == FWD;
                        // Break cycle by creating a temporary rel with a different name (concat rel
                        // and node name) on closing node.
                        auto tmpRel = rewriteQueryRel(queryRel, isCloseOnDst);
                        auto nodeToIntersect =
                            isCloseOnDst ? queryRel.getDstNode() : queryRel.getSrcNode();
                        auto tmpNode = isCloseOnDst ? tmpRel->getDstNode() : tmpRel->getSrcNode();

                        auto planWithFilter = prevPlan->shallowCopy();
                        planExtendFiltersAndScanProperties(
                            *tmpRel, direction, predicates, *planWithFilter);
                        auto nodeIDFilter =
                            createNodeIDComparison(nodeToIntersect->getNodeIDPropertyExpression(),
                                tmpNode->getNodeIDPropertyExpression(),
                                catalog.getBuiltInScalarFunctions());
                        enumerator->appendFilter(nodeIDFilter, *planWithFilter);
                        context->addPlan(newSubgraph, move(planWithFilter));

                        auto planWithIntersect = prevPlan->shallowCopy();
                        planExtendFiltersAndScanProperties(
                            *tmpRel, direction, predicates, *planWithIntersect);
                        if (appendIntersect(nodeToIntersect->getIDProperty(),
                                tmpNode->getIDProperty(), *planWithIntersect)) {
                            context->addPlan(newSubgraph, move(planWithIntersect));
                        }
                    }
                }
            } else {
                for (auto& prevPlan : prevPlans) {
                    auto plan = prevPlan->shallowCopy();
                    planExtendFiltersAndScanProperties(
                        queryRel, isSrcMatched ? FWD : BWD, predicates, *plan);
                    context->addPlan(newSubgraph, move(plan));
                }
            }
        }
    }
}

void JoinOrderEnumerator::planExtendFiltersAndScanProperties(RelExpression& queryRel,
    RelDirection direction, expression_vector& predicates, LogicalPlan& plan) {
    appendExtend(queryRel, direction, plan);
    auto nbrNode = FWD == direction ? queryRel.getDstNode() : queryRel.getSrcNode();
    planFilterAfterExtend(predicates, queryRel, *nbrNode, plan);
    planPropertyScansAfterExtend(queryRel, *nbrNode, plan);
}

void JoinOrderEnumerator::planFilterAfterExtend(
    expression_vector& predicates, RelExpression& rel, NodeExpression& nbrNode, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto nodePropertiesToScan = getPropertiesForVariable(*predicate, nbrNode);
        enumerator->appendScanNodePropIfNecessarySwitch(nodePropertiesToScan, nbrNode, plan);
        auto relPropertiesToScan = getPropertiesForVariable(*predicate, rel);
        enumerator->appendScanRelPropsIfNecessary(relPropertiesToScan, rel, plan);
        enumerator->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansAfterExtend(
    RelExpression& rel, NodeExpression& nbrNode, LogicalPlan& plan) {
    auto nodeProperties = enumerator->getPropertiesForNode(nbrNode);
    enumerator->appendScanNodePropIfNecessarySwitch(nodeProperties, nbrNode, plan);
    auto relProperties = enumerator->getPropertiesForRel(rel);
    enumerator->appendScanRelPropsIfNecessary(relProperties, rel, plan);
}

void JoinOrderEnumerator::planHashJoin() {
    auto currentLevel = context->getCurrentLevel();
    auto queryGraph = context->getQueryGraph();
    for (auto leftSize = currentLevel - 2; leftSize >= ceil(currentLevel / 2.0); --leftSize) {
        auto& subgraphPlansMap = context->getSubqueryGraphPlansMap(leftSize);
        for (auto& [leftSubgraph, leftPlans] : subgraphPlansMap) {
            auto rightSubgraphAndJoinNodePairs =
                queryGraph->getSingleNodeJoiningSubgraph(leftSubgraph, currentLevel - leftSize);
            for (auto& [rightSubgraph, joinNodePos] : rightSubgraphAndJoinNodePairs) {
                // Consider previous example in enumerateExtend()
                // When enumerating second MATCH, and current level = 4
                // we get left subgraph as f, d, e (size = 2), and try to find a connected
                // right subgraph of size 2. A possible right graph could be b, c, d.
                // However, b, c, d is a subgraph enumerated in the first MATCH and has been
                // cleared before enumeration of second MATCH. So subPlansTable does not
                // contain this subgraph.
                if (!context->containPlans(rightSubgraph)) {
                    continue;
                }
                auto& rightPlans = context->getPlans(rightSubgraph);
                auto newSubgraph = leftSubgraph;
                newSubgraph.addSubqueryGraph(rightSubgraph);
                auto& joinNode = *queryGraph->getQueryNode(joinNodePos);
                auto predicates = getNewMatchedExpressions(
                    leftSubgraph, rightSubgraph, newSubgraph, context->getWhereExpressions());
                for (auto& leftPlan : leftPlans) {
                    for (auto& rightPlan : rightPlans) {
                        auto probePlan = leftPlan->shallowCopy();
                        appendLogicalHashJoin(joinNode, *probePlan, *rightPlan);
                        for (auto& predicate : predicates) {
                            enumerator->appendFilter(predicate, *probePlan);
                        }
                        context->addPlan(newSubgraph, move(probePlan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != currentLevel - leftSize) {
                            probePlan = rightPlan->shallowCopy();
                            appendLogicalHashJoin(joinNode, *probePlan, *leftPlan);
                            for (auto& predicate : predicates) {
                                enumerator->appendFilter(predicate, *probePlan);
                            }
                            context->addPlan(newSubgraph, move(probePlan));
                        }
                    }
                }
            }
        }
    }
}

void JoinOrderEnumerator::appendResultScan(
    const expression_vector& expressionsToSelect, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto groupPos = schema->createGroup();
    for (auto& expressionToSelect : expressionsToSelect) {
        schema->insertToGroupAndScope(expressionToSelect, groupPos);
    }
    auto resultScan = make_shared<LogicalResultScan>(expressionsToSelect);
    auto group = schema->getGroup(groupPos);
    group->setEstimatedCardinality(1);
    group->setIsFlat(true);
    plan.appendOperator(move(resultScan));
}

void JoinOrderEnumerator::appendScanNodeID(NodeExpression& queryNode, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto nodeID = queryNode.getIDProperty();
    assert(plan.isEmpty());
    auto scan = make_shared<LogicalScanNodeID>(nodeID, queryNode.getLabel());
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(queryNode.getNodeIDPropertyExpression(), groupPos);
    schema->getGroup(groupPos)->setEstimatedCardinality(
        nodesMetadata.getNodeMetadata(queryNode.getLabel())->getMaxNodeOffset() + 1);
    plan.appendOperator(move(scan));
}

void JoinOrderEnumerator::appendExtend(
    const RelExpression& queryRel, RelDirection direction, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto boundNode = FWD == direction ? queryRel.getSrcNode() : queryRel.getDstNode();
    auto nbrNode = FWD == direction ? queryRel.getDstNode() : queryRel.getSrcNode();
    auto boundNodeID = boundNode->getIDProperty();
    auto nbrNodeID = nbrNode->getIDProperty();
    auto isColumnExtend = catalog.isSingleMultiplicityInDirection(queryRel.getLabel(), direction);
    uint32_t groupPos;
    // If the join is a single (1-hop) fixed-length column extend (e.g., over a relationship with
    // one-to-one multiplicity), then we put the nbrNode vector into the same
    // datachunk/factorization group as the boundNodeId. Otherwise (including a var-length join over
    // a column extend) to a separate data chunk, which will be unflat. However, note that a
    // var-length column join can still write a single value to this unflat nbrNode vector (i.e.,
    // the vector in this case can be an unflat vector with a single value in it).
    if (isColumnExtend && (queryRel.getLowerBound() == 1) &&
        (queryRel.getLowerBound() == queryRel.getUpperBound())) {
        groupPos = schema->getGroupPos(boundNodeID);
    } else {
        auto boundNodeGroupPos = schema->getGroupPos(boundNodeID);
        enumerator->appendFlattenIfNecessary(boundNodeGroupPos, plan);
        groupPos = schema->createGroup();
        schema->getGroup(groupPos)->setEstimatedCardinality(
            schema->getGroup(boundNodeGroupPos)->getEstimatedCardinality() *
            getExtensionRate(boundNode->getLabel(), queryRel.getLabel(), direction));
    }
    auto extend = make_shared<LogicalExtend>(boundNodeID, boundNode->getLabel(), nbrNodeID,
        nbrNode->getLabel(), queryRel.getLabel(), direction, isColumnExtend,
        queryRel.getLowerBound(), queryRel.getUpperBound(), plan.getLastOperator());
    schema->addLogicalExtend(queryRel.getUniqueName(), extend.get());
    schema->insertToGroupAndScope(nbrNode->getNodeIDPropertyExpression(), groupPos);
    plan.appendOperator(move(extend));
}

void JoinOrderEnumerator::appendLogicalHashJoin(
    const NodeExpression& joinNode, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto probePlanSchema = probePlan.getSchema();
    auto joinNodeID = joinNode.getIDProperty();
    // Flat probe side key group if necessary
    auto probeSideKeyGroupPos = probePlanSchema->getGroupPos(joinNodeID);
    auto probeSideKeyGroup = probePlanSchema->getGroup(probeSideKeyGroupPos);
    enumerator->appendFlattenIfNecessary(probeSideKeyGroupPos, probePlan);
    // Merge key group from build side into probe side.
    auto& buildSideSchema = *buildPlan.getSchema();
    auto buildSideKeyGroupPos = buildSideSchema.getGroupPos(joinNodeID);
    probeSideKeyGroup->setEstimatedCardinality(max(probeSideKeyGroup->getEstimatedCardinality(),
        buildSideSchema.getGroup(buildSideKeyGroupPos)->getEstimatedCardinality()));
    probePlanSchema->insertToGroupAndScope(
        buildSideSchema.getExpressionsInScope(buildSideKeyGroupPos), probeSideKeyGroupPos);
    // Merge payload groups
    unordered_set<uint32_t> payloadGroupsPos;
    for (auto& groupPos : buildSideSchema.getGroupsPosInScope()) {
        if (groupPos == buildSideKeyGroupPos) {
            continue;
        }
        payloadGroupsPos.insert(groupPos);
    }
    Enumerator::computeSchemaForSinkOperators(payloadGroupsPos, buildSideSchema, *probePlanSchema);
    auto hashJoin = make_shared<LogicalHashJoin>(joinNodeID, buildSideSchema.copy(),
        buildSideSchema.getExpressionsInScope(), probePlan.getLastOperator(),
        buildPlan.getLastOperator());
    probePlan.appendOperator(move(hashJoin));
}

bool JoinOrderEnumerator::appendIntersect(
    const string& leftNodeID, const string& rightNodeID, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto leftGroupPos = schema->getGroupPos(leftNodeID);
    auto rightGroupPos = schema->getGroupPos(rightNodeID);
    auto& leftGroup = *schema->getGroup(leftGroupPos);
    auto& rightGroup = *schema->getGroup(rightGroupPos);
    if (leftGroup.getIsFlat() || rightGroup.getIsFlat()) {
        // We should use filter close cycle if any group is flat.
        return false;
    }
    auto intersect = make_shared<LogicalIntersect>(leftNodeID, rightNodeID, plan.getLastOperator());
    plan.cost += max(leftGroup.getEstimatedCardinality(), rightGroup.getEstimatedCardinality());
    leftGroup.setEstimatedCardinality(leftGroup.getEstimatedCardinality() * PREDICATE_SELECTIVITY);
    rightGroup.setEstimatedCardinality(
        rightGroup.getEstimatedCardinality() * PREDICATE_SELECTIVITY);
    plan.appendOperator(move(intersect));
    return true;
}

expression_vector JoinOrderEnumerator::getPropertiesForVariable(
    Expression& expression, Expression& variable) {
    expression_vector result;
    for (auto& propertyExpression : expression.getSubPropertyExpressions()) {
        if (propertyExpression->getChild(0)->getUniqueName() != variable.getUniqueName()) {
            continue;
        }
        result.push_back(propertyExpression);
    }
    return result;
}

uint64_t JoinOrderEnumerator::getExtensionRate(
    label_t boundNodeLabel, label_t relLabel, RelDirection relDirection) {
    auto numRels = catalog.getNumRelsForDirectionBoundLabel(relLabel, relDirection, boundNodeLabel);
    return ceil(
        (double)numRels / nodesMetadata.getNodeMetadata(boundNodeLabel)->getMaxNodeOffset() + 1);
}

expression_vector getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const expression_vector& expressions) {
    expression_vector newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getDependentVariableNames();
        if (newSubgraph.containAllVariables(includedVariables) &&
            !prevSubgraph.containAllVariables(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

expression_vector getNewMatchedExpressions(const SubqueryGraph& prevLeftSubgraph,
    const SubqueryGraph& prevRightSubgraph, const SubqueryGraph& newSubgraph,
    const expression_vector& expressions) {
    expression_vector newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getDependentVariableNames();
        if (newSubgraph.containAllVariables(includedVariables) &&
            !prevLeftSubgraph.containAllVariables(includedVariables) &&
            !prevRightSubgraph.containAllVariables(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

shared_ptr<RelExpression> rewriteQueryRel(const RelExpression& queryRel, bool isRewriteDst) {
    auto& nodeToRewrite = isRewriteDst ? *queryRel.getDstNode() : *queryRel.getSrcNode();
    auto tmpNode = make_shared<NodeExpression>(
        nodeToRewrite.getUniqueName() + "_" + queryRel.getUniqueName(), nodeToRewrite.getLabel());
    return make_shared<RelExpression>(queryRel.getUniqueName(), queryRel.getLabel(),
        isRewriteDst ? queryRel.getSrcNode() : tmpNode,
        isRewriteDst ? tmpNode : queryRel.getDstNode(), queryRel.getLowerBound(),
        queryRel.getUpperBound());
}

shared_ptr<Expression> createNodeIDComparison(const shared_ptr<Expression>& left,
    const shared_ptr<Expression>& right, BuiltInVectorOperations* builtInFunctions) {
    expression_vector children;
    children.push_back(left);
    children.push_back(right);
    vector<DataType> childrenTypes;
    childrenTypes.push_back(left->dataType);
    childrenTypes.push_back(right->dataType);
    auto function = builtInFunctions->matchFunction(EQUALS_FUNC_NAME, childrenTypes);
    auto uniqueName = ScalarFunctionExpression::getUniqueName(EQUALS_FUNC_NAME, children);
    return make_shared<ScalarFunctionExpression>(EQUALS, DataType(BOOL), move(children),
        function->execFunc, function->selectFunc, uniqueName);
}

} // namespace planner
} // namespace graphflow
