#include "src/planner/include/enumerator.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/limit/logical_limit.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
#include "src/planner/include/logical_plan/operator/multiplicity_reducer/logical_multiplcity_reducer.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"
#include "src/planner/include/logical_plan/operator/skip/logical_skip.h"

namespace graphflow {
namespace planner {

const double PREDICATE_SELECTIVITY = 0.2;

static uint64_t getExtensionRate(
    label_t boundNodeLabel, label_t relLabel, Direction direction, const Graph& graph);
static SubqueryGraph getFullyMatchedSubqueryGraph(const QueryGraph& queryGraph);

static vector<shared_ptr<Expression>> getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions);
static vector<shared_ptr<Expression>> getNewMatchedExpressions(
    const SubqueryGraph& prevLeftSubgraph, const SubqueryGraph& prevRightSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions);

static vector<shared_ptr<Expression>> extractWhereClause(
    const BoundReadingStatement& readingStatement);
static vector<shared_ptr<Expression>> splitExpressionOnAND(shared_ptr<Expression> expression);
static unordered_set<uint32_t> getUnFlatGroupsPos(
    const Expression& expression, const Schema& schema);
static unordered_set<uint32_t> getUnFlatGroupsPos(const Schema& schema);
static uint32_t getAnyGroupPos(const Expression& expression, const Schema& schema);
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

// Rewrite a query rel that closes a cycle as a regular query rel for extend. This requires giving a
// different identifier to the node that will close the cycle. This identifier is created as rel
// name + node name.
static shared_ptr<RelExpression> rewriteQueryRel(const RelExpression& queryRel, bool isRewriteDst);
static shared_ptr<Expression> createNodeIDEqualComparison(
    const shared_ptr<NodeExpression>& left, const shared_ptr<NodeExpression>& right);

Enumerator::Enumerator(const Graph& graph) : graph{graph} {
    mergedQueryGraph = make_unique<QueryGraph>();
}

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
    // initialize subgraphPlan table to max size (number of query rels)
    subPlansTable = make_unique<SubPlansTable>(singleQuery.getNumQueryRels());
    for (auto& boundQueryPart : singleQuery.boundQueryParts) {
        enumerateQueryPart(*boundQueryPart);
    }
    auto whereClauses = vector<shared_ptr<Expression>>();
    for (auto& readingStatement : singleQuery.boundReadingStatements) {
        for (auto& expression : extractWhereClause(*readingStatement)) {
            whereClauses.push_back(move(expression));
        }
        enumerateReadingStatement(*readingStatement);
    }
    enumerateSubplans(whereClauses);
    enumerateProjectionBody(*singleQuery.boundReturnStatement->getBoundProjectionBody(), true);
    auto& finalPlans =
        subPlansTable->getSubgraphPlans(getFullyMatchedSubqueryGraph(*mergedQueryGraph));
    return move(finalPlans);
}

void Enumerator::enumerateQueryPart(BoundQueryPart& queryPart) {
    auto whereClauses = vector<shared_ptr<Expression>>();
    for (auto& readingStatement : queryPart.boundReadingStatements) {
        for (auto expression : extractWhereClause(*readingStatement)) {
            whereClauses.push_back(move(expression));
        }
        enumerateReadingStatement(*readingStatement);
    }
    if (queryPart.boundWithStatement->hasWhereExpression()) {
        for (auto& expression :
            splitExpressionOnAND(queryPart.boundWithStatement->getWhereExpression())) {
            whereClauses.push_back(move(expression));
        }
    }
    enumerateSubplans(whereClauses);
    // e.g. WITH 1 AS one MATCH ...
    if (!subPlansTable->getSubqueryGraphPlansMap(currentLevel).empty()) {
        enumerateProjectionBody(*queryPart.boundWithStatement->getBoundProjectionBody(), false);
    }
}

void Enumerator::enumerateProjectionBody(
    const BoundProjectionBody& projectionBody, bool isRewritingAllProperties) {
    auto& plans = subPlansTable->getSubgraphPlans(getFullyMatchedSubqueryGraph(*mergedQueryGraph));
    for (auto& plan : plans) {
        appendProjection(
            projectionBody.getProjectionExpressions(), *plan, isRewritingAllProperties);
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

void Enumerator::enumerateReadingStatement(BoundReadingStatement& readingStatement) {
    if (LOAD_CSV_STATEMENT == readingStatement.statementType) {
        enumerateLoadCSVStatement((BoundLoadCSVStatement&)readingStatement);
    } else {
        updateQueryGraph(*((BoundMatchStatement&)readingStatement).queryGraph);
    }
}

void Enumerator::enumerateLoadCSVStatement(const BoundLoadCSVStatement& loadCSVStatement) {
    // LOAD CSV must be the first operator in current implementation
    GF_ASSERT(mergedQueryGraph->isEmpty());
    auto plan = make_unique<LogicalPlan>();
    appendLoadCSV(loadCSVStatement, *plan);
    subPlansTable->addPlan(SubqueryGraph(*mergedQueryGraph), move(plan));
}

void Enumerator::updateQueryGraph(QueryGraph& queryGraph) {
    if (!mergedQueryGraph->isEmpty()) {
        // When entering from one query part to another, subgraphPlans at currentLevel
        // contains only one entry which is the full mergedQueryGraph
        auto& subqueryGraphPlansMap = subPlansTable->getSubqueryGraphPlansMap(currentLevel);
        GF_ASSERT(1 == subqueryGraphPlansMap.size());
        // Keep only plans in the last level and clean cached plans
        subPlansTable->clearUntil(currentLevel);
        matchedQueryRels = subqueryGraphPlansMap.begin()->first.queryRelsSelector;
        matchedQueryNodes = subqueryGraphPlansMap.begin()->first.queryNodesSelector;
    }
    mergedQueryGraph->merge(queryGraph);
    // Restart from level 0 for new query part so that we get hashJoin based plans
    // that uses subplans coming from previous query part.
    // See example in enumerateExtend().
    currentLevel = 0;
}

void Enumerator::enumerateSubplans(const vector<shared_ptr<Expression>>& whereExpressions) {
    // first query part may not have query graph
    // E.g. WITH 1 AS one MATCH (a) ...
    if (mergedQueryGraph->isEmpty()) {
        return;
    }
    enumerateSingleQueryNode(whereExpressions);
    while (currentLevel < mergedQueryGraph->getNumQueryRels()) {
        enumerateNextLevel(whereExpressions);
    }
}

void Enumerator::enumerateSingleQueryNode(const vector<shared_ptr<Expression>>& whereExpressions) {
    auto emptySubgraph = SubqueryGraph(*mergedQueryGraph);
    unique_ptr<LogicalPlan> prevPlan;
    if (subPlansTable->containSubgraphPlans(emptySubgraph)) {
        // Load csv has been previously enumerated and put under emptySubgraph
        auto& prevPlans = subPlansTable->getSubgraphPlans(emptySubgraph);
        GF_ASSERT(1 == prevPlans.size());
        prevPlan = prevPlans[0]->copy();
    } else {
        prevPlan = make_unique<LogicalPlan>();
    }
    if (matchedQueryNodes.count() == 1) {
        // If only single node has been previously enumerated, then join order is decided
        return;
    }
    for (auto nodePos = 0u; nodePos < mergedQueryGraph->getNumQueryNodes(); ++nodePos) {
        auto newSubgraph = SubqueryGraph(*mergedQueryGraph);
        newSubgraph.addQueryNode(nodePos);
        auto plan = prevPlan->copy();
        appendScan(nodePos, *plan);
        for (auto& expression :
            getNewMatchedExpressions(emptySubgraph, newSubgraph, whereExpressions)) {
            appendFilter(expression, *plan);
        }
        subPlansTable->addPlan(newSubgraph, move(plan));
    }
}

void Enumerator::enumerateNextLevel(const vector<shared_ptr<Expression>>& whereExpressions) {
    currentLevel++;
    enumerateExtend(whereExpressions);
    if (currentLevel >= 4) {
        enumerateHashJoin(whereExpressions);
    }
}

void Enumerator::enumerateExtend(const vector<shared_ptr<Expression>>& whereExpressions) {
    auto& subgraphPlansMap = subPlansTable->getSubqueryGraphPlansMap(currentLevel - 1);
    for (auto& [prevSubgraph, prevPlans] : subgraphPlansMap) {
        auto connectedQueryRelsWithDirection =
            mergedQueryGraph->getConnectedQueryRelsWithDirection(prevSubgraph);
        for (auto& [relPos, isSrcConnected, isDstConnected] : connectedQueryRelsWithDirection) {
            // Consider query MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) WITH *
            // MATCH (d)->[r4]->(e)-[r5]->(f) RETURN *
            // First MATCH is enumerated normally. When enumerating second MATCH,
            // we first merge graph as (a)-[r1]->(b)-[r2]->(c)-[r3]->(d)->[r4]->(e)-[r5]->(f)
            // and enumerate from level 0 again. If we hit a query rel that has been
            // previously matched i.e. r1 & r2 & r3, we skip the plan. This guarantees DP only
            // enumerate query rels in the second MATCH.
            // Note this is different from fully merged, since we don't generate plans like
            // build side QVO : a, b, c,  probe side QVO: f, e, d, c, HashJoin(c).
            if (matchedQueryRels[relPos]) {
                continue;
            }
            auto newSubgraph = prevSubgraph;
            newSubgraph.addQueryRel(relPos);
            auto expressionsToFilter =
                getNewMatchedExpressions(prevSubgraph, newSubgraph, whereExpressions);
            auto& queryRel = *mergedQueryGraph->queryRels[relPos];
            if (isSrcConnected && isDstConnected) {
                // Cyclic query is handled as an extend followed by an ID filter. E.g. Consider
                // (a)-[e1]->(b)-[e2]->(c), (a)-[e3]->(c) triangle query, we do regular
                // S(a)->E(e1,b)->E(e2,c) and close the cycle with e3. In order to close, we do a
                // further Extend(e3,c). Note that this c needs a different identifier other than c.
                // We name it as e3_c. Then we apply a filter on c.id=e3_c.id to close the cycle.
                for (auto& prevPlan : prevPlans) {
                    for (auto direction : DIRECTIONS) {
                        auto isRewriteDst = direction == FWD;
                        auto plan = prevPlan->copy();
                        auto tmpRel = rewriteQueryRel(queryRel, isRewriteDst);
                        auto idFilter = createNodeIDEqualComparison(
                            isRewriteDst ? queryRel.dstNode : queryRel.srcNode,
                            isRewriteDst ? tmpRel->dstNode : tmpRel->srcNode);
                        appendExtendAndNecessaryFilters(
                            *tmpRel, direction, expressionsToFilter, *plan);
                        appendFilter(idFilter, *plan);
                        subPlansTable->addPlan(newSubgraph, move(plan));
                    }
                }
            } else {
                for (auto& prevPlan : prevPlans) {
                    auto plan = prevPlan->copy();
                    appendExtendAndNecessaryFilters(
                        queryRel, isSrcConnected ? FWD : BWD, expressionsToFilter, *plan);
                    subPlansTable->addPlan(newSubgraph, move(plan));
                }
            }
        }
    }
}

void Enumerator::enumerateHashJoin(const vector<shared_ptr<Expression>>& whereExpressions) {
    for (auto leftSize = currentLevel - 2; leftSize >= ceil(currentLevel / 2.0); --leftSize) {
        auto& subgraphPlansMap = subPlansTable->getSubqueryGraphPlansMap(leftSize);
        for (auto& [leftSubgraph, leftPlans] : subgraphPlansMap) {
            auto rightSubgraphAndJoinNodePairs = mergedQueryGraph->getSingleNodeJoiningSubgraph(
                leftSubgraph, currentLevel - leftSize);
            for (auto& [rightSubgraph, joinNodePos] : rightSubgraphAndJoinNodePairs) {
                // Consider previous example in enumerateExtend()
                // When enumerating second MATCH, and current level = 4
                // we get left subgraph as f, d, e (size = 2), and try to find a connected
                // right subgraph of size 2. A possible right graph could be b, c, d.
                // However, b, c, d is a subgraph enumerated in the first MATCH and has been
                // cleared before enumeration of second MATCH. So subPlansTable does not
                // contain this subgraph.
                if (!subPlansTable->containSubgraphPlans(rightSubgraph)) {
                    continue;
                }
                auto& rightPlans = subPlansTable->getSubgraphPlans(rightSubgraph);
                auto newSubgraph = leftSubgraph;
                newSubgraph.addSubqueryGraph(rightSubgraph);
                auto expressionsToFilter = getNewMatchedExpressions(
                    leftSubgraph, rightSubgraph, newSubgraph, whereExpressions);
                for (auto& leftPlan : leftPlans) {
                    for (auto& rightPlan : rightPlans) {
                        auto plan = leftPlan->copy();
                        appendLogicalHashJoin(joinNodePos, *rightPlan, *plan);
                        for (auto& expression : expressionsToFilter) {
                            appendFilter(expression, *plan);
                        }
                        subPlansTable->addPlan(newSubgraph, move(plan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != currentLevel - leftSize) {
                            auto planFilpped = rightPlan->copy();
                            appendLogicalHashJoin(joinNodePos, *leftPlan, *planFilpped);
                            for (auto& expression : expressionsToFilter) {
                                appendFilter(expression, *planFilpped);
                            }
                            subPlansTable->addPlan(newSubgraph, move(planFilpped));
                        }
                    }
                }
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

void Enumerator::appendScan(uint32_t queryNodePos, LogicalPlan& plan) {
    auto& queryNode = *mergedQueryGraph->queryNodes[queryNodePos];
    auto nodeID = queryNode.getIDProperty();
    auto scan = plan.lastOperator ?
                    make_shared<LogicalScanNodeID>(nodeID, queryNode.label, plan.lastOperator) :
                    make_shared<LogicalScanNodeID>(nodeID, queryNode.label);
    auto groupPos = plan.schema->createGroup();
    plan.schema->appendToGroup(nodeID, groupPos);
    plan.schema->groups[groupPos]->estimatedCardinality = graph.getNumNodes(queryNode.label);
    plan.appendOperator(move(scan));
}

void Enumerator::appendExtendAndNecessaryFilters(const RelExpression& queryRel, Direction direction,
    const vector<shared_ptr<Expression>>& expressionsToFilter, LogicalPlan& plan) {
    appendExtend(queryRel, direction, plan);
    for (auto& expression : expressionsToFilter) {
        appendFilter(expression, plan);
    }
}

void Enumerator::appendExtend(
    const RelExpression& queryRel, Direction direction, LogicalPlan& plan) {
    auto boundNode = FWD == direction ? queryRel.srcNode : queryRel.dstNode;
    auto nbrNode = FWD == direction ? queryRel.dstNode : queryRel.srcNode;
    auto boundNodeID = boundNode->getIDProperty();
    auto nbrNodeID = nbrNode->getIDProperty();
    auto isColumnExtend =
        graph.getCatalog().isSingleMultiplicityInDirection(queryRel.label, direction);
    uint32_t groupPos;
    if (isColumnExtend) {
        groupPos = plan.schema->getGroupPos(boundNodeID);
    } else {
        auto boundNodeGroupPos = plan.schema->getGroupPos(boundNodeID);
        appendFlatten(boundNodeGroupPos, plan);
        groupPos = plan.schema->createGroup();
        plan.schema->groups[groupPos]->estimatedCardinality =
            plan.schema->groups[boundNodeGroupPos]->estimatedCardinality *
            getExtensionRate(boundNode->label, queryRel.label, direction, graph);
    }
    auto extend = make_shared<LogicalExtend>(boundNodeID, boundNode->label, nbrNodeID,
        nbrNode->label, queryRel.label, direction, isColumnExtend, queryRel.lowerBound,
        queryRel.upperBound, plan.lastOperator);
    plan.schema->addLogicalExtend(queryRel.getInternalName(), extend.get());
    plan.schema->appendToGroup(nbrNodeID, groupPos);
    plan.appendOperator(move(extend));
}

uint32_t Enumerator::appendNecessaryFlattens(
    const unordered_set<uint32_t>& unFlatGroupsPos, LogicalPlan& plan) {
    // randomly pick the first unFlat group to select and flatten the rest
    auto groupPosToSelect = *unFlatGroupsPos.begin();
    for (auto& unFlatGroupPos : unFlatGroupsPos) {
        if (unFlatGroupPos != groupPosToSelect) {
            appendFlatten(unFlatGroupPos, plan);
        }
    }
    return groupPosToSelect;
}

void Enumerator::appendFlatten(uint32_t groupPos, LogicalPlan& plan) {
    auto& group = *plan.schema->groups[groupPos];
    if (group.isFlat) {
        return;
    }
    plan.schema->flattenGroup(groupPos);
    plan.cost += group.estimatedCardinality;
    auto flatten = make_shared<LogicalFlatten>(group.getAnyVariable(), plan.lastOperator);
    plan.appendOperator(move(flatten));
}

void Enumerator::appendLogicalHashJoin(
    uint32_t joinNodePos, LogicalPlan& buildPlan, LogicalPlan& probePlan) {
    auto joinNodeID = mergedQueryGraph->queryNodes[joinNodePos]->getIDProperty();
    // Flat probe side key group if necessary
    auto probeSideKeyGroupPos = probePlan.schema->getGroupPos(joinNodeID);
    auto probeSideKeyGroup = probePlan.schema->groups[probeSideKeyGroupPos].get();
    if (!probeSideKeyGroup->isFlat) {
        appendFlatten(probeSideKeyGroupPos, probePlan);
    }
    // Build side: 1) append non-key vectors in the key data chunk into probe side key data chunk.
    auto buildSideKeyGroupPos = buildPlan.schema->getGroupPos(joinNodeID);
    auto buildSideKeyGroup = buildPlan.schema->groups[buildSideKeyGroupPos].get();
    probeSideKeyGroup->estimatedCardinality =
        max(probeSideKeyGroup->estimatedCardinality, buildSideKeyGroup->estimatedCardinality);
    for (auto& variable : buildSideKeyGroup->variables) {
        if (variable == joinNodeID) {
            continue; // skip the key vector because it exists on probe side
        }
        probePlan.schema->appendToGroup(variable, probeSideKeyGroupPos);
    }

    auto allGroupsOnBuildSideIsFlat = true;
    // the appended probe side group that holds all flat groups on build side
    auto probeSideAppendedFlatGroupPos = UINT32_MAX;
    for (auto i = 0u; i < buildPlan.schema->groups.size(); ++i) {
        if (i == buildSideKeyGroupPos) {
            continue;
        }
        auto& buildSideGroup = buildPlan.schema->groups[i];
        if (buildSideGroup->isFlat) {
            // 2) append flat non-key data chunks into buildSideNonKeyFlatDataChunk.
            if (probeSideAppendedFlatGroupPos == UINT32_MAX) {
                probeSideAppendedFlatGroupPos = probePlan.schema->createGroup();
            }
            probePlan.schema->appendToGroup(*buildSideGroup, probeSideAppendedFlatGroupPos);
        } else {
            // 3) append unflat non-key data chunks as new data chunks into dataChunks.
            allGroupsOnBuildSideIsFlat = false;
            auto groupPos = probePlan.schema->createGroup();
            probePlan.schema->appendToGroup(*buildSideGroup, groupPos);
            probePlan.schema->groups[groupPos]->estimatedCardinality =
                buildSideGroup->estimatedCardinality;
        }
    }

    if (!allGroupsOnBuildSideIsFlat && probeSideAppendedFlatGroupPos != UINT32_MAX) {
        probePlan.schema->flattenGroup(probeSideAppendedFlatGroupPos);
    }
    auto hashJoin = make_shared<LogicalHashJoin>(
        joinNodeID, buildPlan.lastOperator, buildPlan.schema->copy(), probePlan.lastOperator);
    probePlan.appendOperator(move(hashJoin));
}

void Enumerator::appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan) {
    appendNecessaryScans(*expression, plan);
    auto unFlatGroupsPos = getUnFlatGroupsPos(*expression, *plan.schema);
    uint32_t groupPosToSelect = unFlatGroupsPos.empty() ?
                                    getAnyGroupPos(*expression, *plan.schema) :
                                    appendNecessaryFlattens(unFlatGroupsPos, plan);
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
        appendNecessaryScans(*expression, plan);
        auto unFlatGroupsPos = getUnFlatGroupsPos(*expression, *plan.schema);
        uint32_t groupPosToWrite = unFlatGroupsPos.empty() ?
                                       getAnyGroupPos(*expression, *plan.schema) :
                                       appendNecessaryFlattens(unFlatGroupsPos, plan);
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

void Enumerator::appendNecessaryScans(const Expression& expression, LogicalPlan& plan) {
    for (auto& tmp : expression.getIncludedExpressions(unordered_set<ExpressionType>{PROPERTY})) {
        auto& propertyExpression = (const PropertyExpression&)*tmp;
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
                                appendNecessaryFlattens(unFlatGroupsPos, plan);
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
                                appendNecessaryFlattens(unFlatGroupsPos, plan);
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

uint64_t getExtensionRate(
    label_t boundNodeLabel, label_t relLabel, Direction direction, const Graph& graph) {
    auto numRels = graph.getNumRelsForDirBoundLabelRelLabel(direction, boundNodeLabel, relLabel);
    return ceil((double)numRels / graph.getNumNodes(boundNodeLabel));
}

SubqueryGraph getFullyMatchedSubqueryGraph(const QueryGraph& queryGraph) {
    auto matchedSubgraph = SubqueryGraph(queryGraph);
    for (auto i = 0u; i < queryGraph.getNumQueryNodes(); ++i) {
        matchedSubgraph.addQueryNode(i);
    }
    for (auto i = 0u; i < queryGraph.getNumQueryRels(); ++i) {
        matchedSubgraph.addQueryRel(i);
    }
    return matchedSubgraph;
}

vector<shared_ptr<Expression>> getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions) {
    vector<shared_ptr<Expression>> newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getIncludedVariableNames();
        if (newSubgraph.containAllVariables(includedVariables) &&
            !prevSubgraph.containAllVariables(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

vector<shared_ptr<Expression>> getNewMatchedExpressions(const SubqueryGraph& prevLeftSubgraph,
    const SubqueryGraph& prevRightSubgraph, const SubqueryGraph& newSubgraph,
    const vector<shared_ptr<Expression>>& expressions) {
    vector<shared_ptr<Expression>> newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getIncludedVariableNames();
        if (newSubgraph.containAllVariables(includedVariables) &&
            !prevLeftSubgraph.containAllVariables(includedVariables) &&
            !prevRightSubgraph.containAllVariables(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

vector<shared_ptr<Expression>> extractWhereClause(const BoundReadingStatement& readingStatement) {
    if (readingStatement.statementType == LOAD_CSV_STATEMENT) {
        return vector<shared_ptr<Expression>>();
    } else {
        GF_ASSERT(readingStatement.statementType == MATCH_STATEMENT);
        auto& matchStatement = (const BoundMatchStatement&)readingStatement;
        return matchStatement.whereExpression ?
                   splitExpressionOnAND(matchStatement.whereExpression) :
                   vector<shared_ptr<Expression>>();
    }
}

vector<shared_ptr<Expression>> splitExpressionOnAND(shared_ptr<Expression> expression) {
    auto result = vector<shared_ptr<Expression>>();
    if (AND == expression->expressionType) {
        for (auto& child : expression->children) {
            for (auto& exp : splitExpressionOnAND(child)) {
                result.push_back(exp);
            }
        }
    } else {
        result.push_back(expression);
    }
    return result;
}

unordered_set<uint32_t> getUnFlatGroupsPos(const Expression& expression, const Schema& schema) {
    auto subExpressions = expression.getIncludedExpressions(
        unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT, ALIAS});
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
        if (!schema.groups[0]->isFlat) {
            unFlatGroupsPos.insert(i);
        }
    }
    return unFlatGroupsPos;
}

uint32_t getAnyGroupPos(const Expression& expression, const Schema& schema) {
    auto subExpressions = expression.getIncludedExpressions(
        unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT, ALIAS});
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

shared_ptr<RelExpression> rewriteQueryRel(const RelExpression& queryRel, bool isRewriteDst) {
    auto& nodeToRewrite = isRewriteDst ? *queryRel.dstNode : *queryRel.srcNode;
    auto tmpNode = make_shared<NodeExpression>(
        nodeToRewrite.getInternalName() + "_" + queryRel.getInternalName(), nodeToRewrite.label);
    return make_shared<RelExpression>(queryRel.getInternalName(), queryRel.label,
        isRewriteDst ? queryRel.srcNode : tmpNode, isRewriteDst ? tmpNode : queryRel.dstNode,
        queryRel.lowerBound, queryRel.upperBound);
}

shared_ptr<Expression> createNodeIDEqualComparison(
    const shared_ptr<NodeExpression>& left, const shared_ptr<NodeExpression>& right) {
    auto srcNodeID = make_shared<PropertyExpression>(
        NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX /* property key for internal id */, left);
    auto dstNodeID = make_shared<PropertyExpression>(
        NODE_ID, INTERNAL_ID_SUFFIX, UINT32_MAX /* property key for internal id  */, right);
    auto result = make_shared<Expression>(EQUALS_NODE_ID, BOOL, move(srcNodeID), move(dstNodeID));
    result->rawExpression =
        result->children[0]->getInternalName() + " = " + result->children[1]->getInternalName();
    return result;
}

} // namespace planner
} // namespace graphflow
