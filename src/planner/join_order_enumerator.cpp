#include "src/planner/include/join_order_enumerator.h"

#include "src/planner/include/enumerator.h"
#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/planner/logical_plan/logical_operator/include/logical_result_scan.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"
#include "src/planner/logical_plan/logical_operator/include/sink_util.h"

namespace graphflow {
namespace planner {

static expression_vector getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const expression_vector& expressions);
static expression_vector getNewMatchedExpressions(const SubqueryGraph& prevLeftSubgraph,
    const SubqueryGraph& prevRightSubgraph, const SubqueryGraph& newSubgraph,
    const expression_vector& expressions);

vector<unique_ptr<LogicalPlan>> JoinOrderEnumerator::enumerateJoinOrder(
    const QueryGraph& queryGraph, const shared_ptr<Expression>& queryGraphPredicate,
    vector<unique_ptr<LogicalPlan>> prevPlans) {
    context->init(queryGraph, queryGraphPredicate, move(prevPlans));
    context->hasExpressionsToScanFromOuter() ? planResultScan() : planNodeScan();
    context->currentLevel++;
    while (context->currentLevel < context->maxLevel) {
        planCurrentLevel();
        context->currentLevel++;
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
        auto node = queryGraph->getQueryNode(nodePos);
        appendScanNodeID(node, *plan);
        auto predicates =
            getNewMatchedExpressions(emptySubgraph, newSubgraph, context->getWhereExpressions());
        planFiltersForNode(predicates, *node, *plan);
        planPropertyScansForNode(*node, *plan);
        context->addPlan(newSubgraph, move(plan));
    }
}

void JoinOrderEnumerator::planFiltersForNode(
    expression_vector& predicates, NodeExpression& node, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto propertiesToScan = getPropertiesForVariable(*predicate, node);
        enumerator->appendScanNodePropIfNecessarySwitch(propertiesToScan, node, plan);
        enumerator->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansForNode(NodeExpression& node, LogicalPlan& plan) {
    auto properties = enumerator->getPropertiesForNode(node);
    enumerator->appendScanNodePropIfNecessarySwitch(properties, node, plan);
}

void JoinOrderEnumerator::planCurrentLevel() {
    assert(context->currentLevel > 1);
    planINLJoin();
    planHashJoin();
    context->subPlansTable->finalizeLevel(context->currentLevel);
}

void JoinOrderEnumerator::planINLJoin() {
    auto prevLevel = context->currentLevel - 1;
    for (auto& [subgraph, plans] : *context->subPlansTable->getSubqueryGraphPlansMap(prevLevel)) {
        auto nodeNbrPositions = subgraph.getNodeNbrPositions();
        for (auto& nodePos : nodeNbrPositions) {
            planNodeINLJoin(subgraph, nodePos, plans);
        }
        auto relNbrPositions = subgraph.getRelNbrPositions();
        for (auto& relPos : relNbrPositions) {
            planRelINLJoin(subgraph, relPos, plans);
        }
    }
}

void JoinOrderEnumerator::planNodeINLJoin(const SubqueryGraph& prevSubgraph, uint32_t nodePos,
    vector<unique_ptr<LogicalPlan>>& prevPlans) {
    auto newSubgraph = prevSubgraph;
    newSubgraph.addQueryNode(nodePos);
    auto node = context->mergedQueryGraph->getQueryNode(nodePos);
    auto predicates =
        getNewMatchedExpressions(prevSubgraph, newSubgraph, context->getWhereExpressions());
    for (auto& prevPlan : prevPlans) {
        auto plan = prevPlan->shallowCopy();
        planFiltersForNode(predicates, *node, *plan);
        planPropertyScansForNode(*node, *plan);
        plan->multiplyCost(EnumeratorKnobs::RANDOM_LOOKUP_PENALTY);
        context->addPlan(newSubgraph, move(plan));
    }
}

void JoinOrderEnumerator::planRelINLJoin(const SubqueryGraph& prevSubgraph, uint32_t relPos,
    vector<unique_ptr<LogicalPlan>>& prevPlans) {
    // Consider query MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) WITH *
    // MATCH (d)->[r4]->(e)-[r5]->(f) RETURN *
    // First MATCH is enumerated normally. When enumerating second MATCH,
    // we first merge graph as (a)-[r1]->(b)-[r2]->(c)-[r3]->(d)->[r4]->(e)-[r5]->(f) and
    // enumerate from level 0 again. If we hit a query rel that has been previously matched
    // i.e. r1 & r2 & r3, we skip the plan. This guarantees DP only enumerate query rels in
    // the second MATCH. Note this is different from fully merged, since we don't generate
    // plans like build side QVO : a, b, c,  probe side QVO: f, e, d, c, HashJoin(c).
    if (context->matchedQueryRels[relPos]) {
        return;
    }
    auto newSubgraph = prevSubgraph;
    newSubgraph.addQueryRel(relPos);
    auto rel = context->mergedQueryGraph->getQueryRel(relPos);
    auto predicates =
        getNewMatchedExpressions(prevSubgraph, newSubgraph, context->getWhereExpressions());
    auto isSrcConnected = prevSubgraph.isSrcConnected(relPos);
    auto isDstConnected = prevSubgraph.isDstConnected(relPos);
    assert(isSrcConnected || isDstConnected);
    auto direction = isSrcConnected ? FWD : BWD;
    for (auto& prevPlan : prevPlans) {
        auto plan = prevPlan->shallowCopy();
        appendExtend(rel, direction, *plan);
        planFiltersForRel(predicates, *rel, direction, *plan);
        planPropertyScansForRel(*rel, direction, *plan);
        context->addPlan(newSubgraph, move(plan));
    }
}

void JoinOrderEnumerator::planFiltersForRel(
    expression_vector& predicates, RelExpression& rel, RelDirection direction, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        auto relPropertiesToScan = getPropertiesForVariable(*predicate, rel);
        enumerator->appendScanRelPropsIfNecessary(relPropertiesToScan, rel, direction, plan);
        enumerator->appendFilter(predicate, plan);
    }
}

void JoinOrderEnumerator::planPropertyScansForRel(
    RelExpression& rel, RelDirection direction, LogicalPlan& plan) {
    auto relProperties = enumerator->getPropertiesForRel(rel);
    enumerator->appendScanRelPropsIfNecessary(relProperties, rel, direction, plan);
}

void JoinOrderEnumerator::planHashJoin(uint32_t leftLevel, uint32_t rightLevel) {
    assert(leftLevel <= rightLevel);
    auto rightSubgraphPlansMap = context->subPlansTable->getSubqueryGraphPlansMap(rightLevel);
    for (auto& [rightSubgraph, rightPlans] : *rightSubgraphPlansMap) {
        for (auto& nbrSubgraph : rightSubgraph.getNbrSubgraphs(leftLevel)) {
            // Consider previous example in enumerateExtend(), when enumerating second MATCH, and
            // current level = 4 we get left subgraph as f, d, e (size = 2), and try to find a
            // connected right subgraph of size 2. A possible right graph could be b, c, d. However,
            // b, c, d is a subgraph enumerated in the first MATCH and has been cleared before
            // enumeration of second MATCH. So subPlansTable does not contain this subgraph.
            if (!context->containPlans(nbrSubgraph)) {
                continue;
            }
            auto joinNodePositions = rightSubgraph.getConnectedNodePos(nbrSubgraph);
            assert(joinNodePositions.size() == 1);
            auto joinNode = context->mergedQueryGraph->getQueryNode(joinNodePositions[0]);
            auto& leftPlans = context->getPlans(nbrSubgraph);
            auto newSubgraph = rightSubgraph;
            newSubgraph.addSubqueryGraph(nbrSubgraph);
            auto predicates = getNewMatchedExpressions(
                nbrSubgraph, rightSubgraph, newSubgraph, context->getWhereExpressions());
            for (auto& leftPlan : leftPlans) {
                for (auto& rightPlan : rightPlans) {
                    auto leftPlanProbeCopy = leftPlan->shallowCopy();
                    auto rightPlanBuildCopy = rightPlan->shallowCopy();
                    auto leftPlanBuildCopy = leftPlan->shallowCopy();
                    auto rightPlanProbeCopy = rightPlan->shallowCopy();
                    appendHashJoin(joinNode, *leftPlanProbeCopy, *rightPlanBuildCopy);
                    planFiltersForHashJoin(predicates, *leftPlanProbeCopy);
                    context->addPlan(newSubgraph, move(leftPlanProbeCopy));
                    // flip build and probe side to get another HashJoin plan
                    if (leftLevel != rightLevel) {
                        appendHashJoin(joinNode, *rightPlanProbeCopy, *leftPlanBuildCopy);
                        planFiltersForHashJoin(predicates, *rightPlanProbeCopy);
                        context->addPlan(newSubgraph, move(rightPlanProbeCopy));
                    }
                }
            }
        }
    }
}

void JoinOrderEnumerator::planFiltersForHashJoin(expression_vector& predicates, LogicalPlan& plan) {
    for (auto& predicate : predicates) {
        enumerator->appendFilter(predicate, plan);
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
    group->setIsFlat(true);
    plan.appendOperator(move(resultScan));
}

void JoinOrderEnumerator::appendScanNodeID(shared_ptr<NodeExpression>& node, LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto schema = plan.getSchema();
    auto scan = make_shared<LogicalScanNodeID>(node);
    scan->computeSchema(*schema);
    plan.appendOperator(move(scan));
    // update cardinality estimation info
    auto numNodes = nodesMetadata.getNodeMetadata(node->getLabel())->getMaxNodeOffset() + 1;
    schema->getGroup(node->getIDProperty())->setMultiplier(numNodes);
}

void JoinOrderEnumerator::appendExtend(
    shared_ptr<RelExpression>& rel, RelDirection direction, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto boundNode = FWD == direction ? rel->getSrcNode() : rel->getDstNode();
    auto nbrNode = FWD == direction ? rel->getDstNode() : rel->getSrcNode();
    auto isManyToOne =
        catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(rel->getLabel(), direction);
    // Note that a var-length column join writes a single value to unflat nbrNode
    // vector (i.e., this vector is an unflat vector with a single value in it).
    auto isColumn = isManyToOne && !rel->isVariableLength();
    if (!isColumn) {
        Enumerator::appendFlattenIfNecessary(boundNode->getNodeIDPropertyExpression(), plan);
    }
    auto extend = make_shared<LogicalExtend>(boundNode, nbrNode, rel->getLabel(), direction,
        isColumn, rel->getLowerBound(), rel->getUpperBound(), plan.getLastOperator());
    extend->computeSchema(*schema);
    plan.appendOperator(move(extend));
    // update cardinality estimation info
    if (!isColumn) {
        auto extensionRate = getExtensionRate(boundNode->getLabel(), rel->getLabel(), direction);
        schema->getGroup(nbrNode->getIDProperty())->setMultiplier(extensionRate);
    }
    plan.increaseCost(plan.getCardinality());
}

static bool isJoinKeyUniqueOnBuildSide(const string& joinNodeID, LogicalPlan& buildPlan) {
    auto buildSchema = buildPlan.getSchema();
    auto numGroupsInScope = buildSchema->getGroupsPosInScope().size();
    bool hasProjectedOutGroups = buildSchema->getNumGroups() > numGroupsInScope;
    if (numGroupsInScope > 1 || hasProjectedOutGroups) {
        return false;
    }
    // Now there is a single factorization group, we need to further make sure joinNodeID comes from
    // ScanNodeID operator. Because if joinNodeID comes from a ColExtend we cannot guarantee the
    // reverse mapping is still many-to-one. We look for the most simple pattern where build plan is
    // linear.
    auto firstop = buildPlan.getLastOperator().get();
    while (firstop->getNumChildren() != 0) {
        if (firstop->getNumChildren() > 1) {
            return false;
        }
        firstop = firstop->getChild(0).get();
    }
    if (firstop->getLogicalOperatorType() != LOGICAL_SCAN_NODE_ID) {
        return false;
    }
    auto scanNodeID = (LogicalScanNodeID*)firstop;
    if (scanNodeID->getNodeExpression()->getIDProperty() != joinNodeID) {
        return false;
    }
    return true;
}

static bool buildSideHasNoPayload(const Schema& buildSchema) {
    return buildSchema.getExpressionsInScope().size() == 1;
}

static bool buildSideHasUnFlatPayload(const string& key, const Schema& buildSchema) {
    for (auto groupPos : SinkOperatorUtil::getGroupsPosIgnoringKeyGroup(buildSchema, key)) {
        if (!buildSchema.getGroup(groupPos)->getIsFlat()) {
            return true;
        }
    }
    return false;
}

// Key group contains expressions other than key, i.e. more than one expression.
static bool buildSideKeyGroupIsNotTrivial(const string& key, const Schema& buildSchema) {
    auto keyGroupPos = buildSchema.getGroupPos(key);
    return buildSchema.getExpressionsInScope(keyGroupPos).size() > 1;
}

void JoinOrderEnumerator::appendHashJoin(
    const shared_ptr<NodeExpression>& joinNode, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto joinNodeID = joinNode->getIDProperty();
    auto& buildSideSchema = *buildPlan.getSchema();
    auto probeSideSchema = probePlan.getSchema();
    auto probeSideKeyGroupPos = probeSideSchema->getGroupPos(joinNodeID);
    probePlan.increaseCost(probePlan.getCardinality() + buildPlan.getCardinality());
    // Flat probe side key group if the build side contains more than one group or the build side
    // has projected out data chunks, which may increase the multiplicity of data chunks in the
    // build side. The core idea is to keep probe side key unflat only when we know that there is
    // only 0 or 1 match for each key.
    if (!isJoinKeyUniqueOnBuildSide(joinNodeID, buildPlan)) {
        Enumerator::appendFlattenIfNecessary(probeSideKeyGroupPos, probePlan);
        // Update probe side cardinality if build side does not guarantee to have 0/1 match
        probePlan.multiplyCardinality(
            buildPlan.getCardinality() * EnumeratorKnobs::PREDICATE_SELECTIVITY);
        probePlan.multiplyCost(EnumeratorKnobs::FLAT_PROBE_PENALTY);
    }
    // Analyze if we can scan multiple rows or not when the probe key if flat.
    // TODO(Guodong): some of the follwing logics are ideally being replaced by duplicating probing
    // key as in vectorized processing. Also, we might get rid of flattening probe key logic.
    bool isScanOneRow = false;
    if (probeSideSchema->getGroup(probeSideKeyGroupPos)->getIsFlat()) {
        // If there is no payload, we can't represent more than one match in an unFlat vector, i.e.
        // the multiplicity information is lost in the output.
        isScanOneRow |= buildSideHasNoPayload(buildSideSchema);
        // Factorization structure limitation.
        isScanOneRow |= buildSideHasUnFlatPayload(joinNodeID, buildSideSchema);
        // If key group is non-trivial, i.e. contains payload vectors along side with key vector, we
        // cannot guarantee these payload vectors has 1-1 mapping with the key (e.g. edge
        // properties). Therefore, we cannot scan multiple rows for a single key.
        isScanOneRow |= buildSideKeyGroupIsNotTrivial(joinNodeID, buildSideSchema);
    }
    auto numGroupsBeforeMerging = probeSideSchema->getNumGroups();
    SinkOperatorUtil::mergeSchema(buildSideSchema, *probeSideSchema, joinNodeID, isScanOneRow);
    vector<uint64_t> flatOutputGroupPositions;
    for (auto i = numGroupsBeforeMerging; i < probeSideSchema->getNumGroups(); ++i) {
        if (probeSideSchema->getGroup(i)->getIsFlat()) {
            flatOutputGroupPositions.push_back(i);
        }
    }
    auto hashJoin = make_shared<LogicalHashJoin>(joinNode, buildSideSchema.copy(),
        flatOutputGroupPositions, buildSideSchema.getExpressionsInScope(), isScanOneRow,
        probePlan.getLastOperator(), buildPlan.getLastOperator());
    probePlan.appendOperator(move(hashJoin));
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
    auto numRels = catalog.getReadOnlyVersion()->getNumRelsForDirectionBoundLabel(
        relLabel, relDirection, boundNodeLabel);
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

} // namespace planner
} // namespace graphflow
