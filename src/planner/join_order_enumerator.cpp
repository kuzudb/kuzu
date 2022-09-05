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
    assert(!context->hasExpressionsToScanFromOuter());
    planTableScan();
    context->currentLevel++;
    while (context->currentLevel < context->maxLevel) {
        planLevel(context->currentLevel++);
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
    auto queryGraph = context->getQueryGraph();
    for (auto nodePos = 0u; nodePos < queryGraph->getNumQueryNodes(); ++nodePos) {
        if (context->matchedQueryNodes[nodePos]) {
            continue;
        }
        auto newSubgraph = context->getEmptySubqueryGraph();
        newSubgraph.addQueryNode(nodePos);
        auto plan = make_unique<LogicalPlan>();
        auto node = queryGraph->getQueryNode(nodePos);
        appendScanNodeID(node, *plan);
        auto predicates = getNewMatchedExpressions(
            context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
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

void JoinOrderEnumerator::planRelScan() {
    auto queryGraph = context->getQueryGraph();
    for (auto relPos = 0u; relPos < queryGraph->getNumQueryRels(); ++relPos) {
        if (context->matchedQueryRels[relPos]) {
            continue;
        }
        auto rel = queryGraph->getQueryRel(relPos);
        auto newSubgraph = context->getEmptySubqueryGraph();
        newSubgraph.addQueryRel(relPos);
        auto predicates = getNewMatchedExpressions(
            context->getEmptySubqueryGraph(), newSubgraph, context->getWhereExpressions());
        for (auto direction : REL_DIRECTIONS) {
            auto plan = make_unique<LogicalPlan>();
            auto boundNode = direction == FWD ? rel->getSrcNode() : rel->getDstNode();
            appendScanNodeID(boundNode, *plan);
            planRelExtendFiltersAndProperties(rel, direction, predicates, *plan);
            context->addPlan(newSubgraph, move(plan));
        }
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

void JoinOrderEnumerator::planJoin(uint32_t leftLevel, uint32_t rightLevel) {
    assert(leftLevel <= rightLevel);
    for (auto& rightSubgraph : context->subPlansTable->getSubqueryGraphs(rightLevel)) {
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
            // If index nested loop (INL) join is possible, we prune hash join plans
            if (canApplyINLJoin(rightSubgraph, nbrSubgraph, joinNode)) {
                planINLJoin(rightSubgraph, nbrSubgraph, joinNode);
            } else if (canApplyINLJoin(nbrSubgraph, rightSubgraph, joinNode)) {
                planINLJoin(nbrSubgraph, rightSubgraph, joinNode);
            } else {
                planHashJoin(rightSubgraph, nbrSubgraph, joinNode, leftLevel != rightLevel);
            }
        }
    }
}

static LogicalOperator* getCurrentPipelineSourceOperator(LogicalPlan& plan) {
    auto op = plan.getLastOperator().get();
    while (
        op->getNumChildren() == 1) { // We break pipeline for any operator with more than one child
        op = op->getChild(0).get();
    }
    assert(op != nullptr);
    return op;
}

// Return the node whose ID has sequential guarantee on the plan.
static shared_ptr<NodeExpression> getSequentialNode(LogicalPlan& plan) {
    auto pipelineSource = getCurrentPipelineSourceOperator(plan);
    if (pipelineSource->getLogicalOperatorType() != LOGICAL_SCAN_NODE_ID) {
        // Pipeline source is not ScanNodeID, meaning at least one sink has happened (e.g. HashJoin)
        // and we loose any sequential guarantees.
        return nullptr;
    }
    return ((LogicalScanNodeID*)pipelineSource)->getNodeExpression();
}

// Check whether given node ID has sequential guarantee on the plan.
static bool isNodeSequential(LogicalPlan& plan, NodeExpression* node) {
    auto sequentialNode = getSequentialNode(plan);
    return sequentialNode != nullptr && sequentialNode->getUniqueName() == node->getUniqueName();
}

// We apply index nested loop join if the following to conditions are satisfied
// - otherSubgraph is an edge; and
// - join node is sequential on at least one plan corresponding to subgraph. (Otherwise INLJ will
// trigger non-sequential read).
bool JoinOrderEnumerator::canApplyINLJoin(const SubqueryGraph& subgraph,
    const SubqueryGraph& otherSubgraph, shared_ptr<NodeExpression> joinNode) {
    if (!otherSubgraph.isSingleRel()) {
        return false;
    }
    for (auto& plan : context->getPlans(subgraph)) {
        if (isNodeSequential(*plan, joinNode.get())) {
            return true;
        }
    }
    return false;
}

static uint32_t extractJoinRelPos(const SubqueryGraph& subgraph, const QueryGraph& queryGraph) {
    for (auto relPos = 0u; relPos < queryGraph.getNumQueryRels(); ++relPos) {
        if (subgraph.queryRelsSelector[relPos]) {
            return relPos;
        }
    }
    assert(false);
}

void JoinOrderEnumerator::planINLJoin(const SubqueryGraph& subgraph,
    const SubqueryGraph& otherSubgraph, shared_ptr<NodeExpression> joinNode) {
    assert(otherSubgraph.getNumQueryRels() == 1);
    auto queryGraph = context->getQueryGraph();
    auto relPos = extractJoinRelPos(otherSubgraph, *queryGraph);
    auto rel = queryGraph->getQueryRel(relPos);
    auto newSubgraph = subgraph;
    newSubgraph.addQueryRel(relPos);
    auto predicates =
        getNewMatchedExpressions(subgraph, newSubgraph, context->getWhereExpressions());
    for (auto& prevPlan : context->getPlans(subgraph)) {
        if (isNodeSequential(*prevPlan, joinNode.get())) {
            auto plan = prevPlan->shallowCopy();
            auto direction = joinNode->getUniqueName() == rel->getSrcNodeName() ? FWD : BWD;
            planRelExtendFiltersAndProperties(rel, direction, predicates, *plan);
            context->addPlan(newSubgraph, move(plan));
        }
    }
}

void JoinOrderEnumerator::planHashJoin(const SubqueryGraph& subgraph,
    const SubqueryGraph& otherSubgraph, shared_ptr<NodeExpression> joinNode, bool flipPlan) {
    auto newSubgraph = subgraph;
    newSubgraph.addSubqueryGraph(otherSubgraph);
    auto predicates = getNewMatchedExpressions(
        subgraph, otherSubgraph, newSubgraph, context->getWhereExpressions());
    for (auto& leftPlan : context->getPlans(subgraph)) {
        for (auto& rightPlan : context->getPlans(otherSubgraph)) {
            auto leftPlanProbeCopy = leftPlan->shallowCopy();
            auto rightPlanBuildCopy = rightPlan->shallowCopy();
            auto leftPlanBuildCopy = leftPlan->shallowCopy();
            auto rightPlanProbeCopy = rightPlan->shallowCopy();
            appendHashJoin(joinNode, *leftPlanProbeCopy, *rightPlanBuildCopy);
            planFiltersForHashJoin(predicates, *leftPlanProbeCopy);
            context->addPlan(newSubgraph, move(leftPlanProbeCopy));
            // flip build and probe side to get another HashJoin plan
            if (flipPlan) {
                appendHashJoin(joinNode, *rightPlanProbeCopy, *leftPlanBuildCopy);
                planFiltersForHashJoin(predicates, *rightPlanProbeCopy);
                context->addPlan(newSubgraph, move(rightPlanProbeCopy));
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
    auto numNodes = nodesMetadata.getNodeMetadata(node->getTableID())->getMaxNodeOffset() + 1;
    schema->getGroup(node->getIDProperty())->setMultiplier(numNodes);
}

void JoinOrderEnumerator::appendExtend(
    shared_ptr<RelExpression>& rel, RelDirection direction, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto boundNode = FWD == direction ? rel->getSrcNode() : rel->getDstNode();
    auto nbrNode = FWD == direction ? rel->getDstNode() : rel->getSrcNode();
    auto isManyToOne =
        catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(rel->getTableID(), direction);
    // Note that a var-length column join writes a single value to unflat nbrNode
    // vector (i.e., this vector is an unflat vector with a single value in it).
    auto isColumn = isManyToOne && !rel->isVariableLength();
    if (!isColumn) {
        Enumerator::appendFlattenIfNecessary(boundNode->getNodeIDPropertyExpression(), plan);
    }
    auto extend = make_shared<LogicalExtend>(boundNode, nbrNode, rel->getTableID(), direction,
        isColumn, rel->getLowerBound(), rel->getUpperBound(), plan.getLastOperator());
    extend->computeSchema(*schema);
    plan.appendOperator(move(extend));
    // update cardinality estimation info
    if (!isColumn) {
        auto extensionRate =
            getExtensionRate(boundNode->getTableID(), rel->getTableID(), direction);
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
    table_id_t boundTableID, table_id_t relTableID, RelDirection relDirection) {
    auto numRels = catalog.getReadOnlyVersion()->getNumRelsForDirectionBoundTableID(
        relTableID, relDirection, boundTableID);
    return ceil(
        (double)numRels / nodesMetadata.getNodeMetadata(boundTableID)->getMaxNodeOffset() + 1);
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
