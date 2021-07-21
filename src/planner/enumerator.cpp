#include "src/planner/include/enumerator.h"

#include "src/binder/include/expression/function_expression.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/flatten/logical_flatten.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"

const double PREDICATE_SELECTIVITY = 0.2;

namespace graphflow {
namespace planner {

static uint64_t getExtensionRate(
    label_t boundNodeLabel, label_t relLabel, Direction direction, const Graph& graph);

static vector<shared_ptr<Expression>> getNewMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions);
static vector<shared_ptr<Expression>> getNewMatchedExpressions(
    const SubqueryGraph& prevLeftSubgraph, const SubqueryGraph& prevRightSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions);

static vector<shared_ptr<Expression>> extractWhereClause(
    const BoundReadingStatement& readingStatement);
static vector<shared_ptr<Expression>> splitExpressionOnAND(shared_ptr<Expression> expression);
static unordered_map<uint32_t, string> getUnFlatGroupsPos(
    shared_ptr<Expression> expression, Schema& schema);

static vector<shared_ptr<Expression>> rewriteExpressionToProject(
    vector<shared_ptr<Expression>> expressions, const Catalog& catalog,
    bool isRewritingAllProperties);
static vector<shared_ptr<Expression>> rewriteVariableExpression(
    shared_ptr<Expression> expression, const Catalog& catalog, bool isRewritingAllProperties);
static vector<shared_ptr<Expression>> createLogicalPropertyExpressions(
    shared_ptr<Expression> expression, const vector<PropertyDefinition>& properties);

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
    auto& finalPlans = enumerateProjectionStatement(singleQuery.boundReturnStatement->expressions);
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
    if (queryPart.boundWithStatement->whereExpression) {
        for (auto& expression :
            splitExpressionOnAND(queryPart.boundWithStatement->whereExpression)) {
            whereClauses.push_back(move(expression));
        }
    }
    enumerateSubplans(whereClauses);
    // e.g. WITH 1 AS one MATCH ...
    if (!subPlansTable->getSubqueryGraphPlansMap(currentLevel).empty()) {
        enumerateProjectionStatement(queryPart.boundWithStatement->expressions);
    }
}

vector<unique_ptr<LogicalPlan>>& Enumerator::enumerateProjectionStatement(
    const vector<shared_ptr<Expression>>& expressionsToProject) {
    auto matchedSubgraph = SubqueryGraph(*mergedQueryGraph);
    for (auto i = 0u; i < mergedQueryGraph->getNumQueryNodes(); ++i) {
        matchedSubgraph.addQueryNode(i);
    }
    for (auto i = 0u; i < mergedQueryGraph->getNumQueryRels(); ++i) {
        matchedSubgraph.addQueryRel(i);
    }
    auto& plans = subPlansTable->getSubgraphPlans(matchedSubgraph);
    for (auto& plan : plans) {
        appendProjection(expressionsToProject, *plan);
    }
    return plans;
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

void Enumerator::enumerateSubplans(const vector<shared_ptr<Expression>>& whereClause) {
    // first query part may not have query graph
    // E.g. WITH 1 AS one MATCH (a) ...
    if (mergedQueryGraph->isEmpty()) {
        return;
    }
    enumerateSingleQueryNode(whereClause);
    while (currentLevel < mergedQueryGraph->getNumQueryRels()) {
        enumerateNextLevel(whereClause);
    }
}

void Enumerator::enumerateSingleQueryNode(const vector<shared_ptr<Expression>>& whereClause) {
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
        for (auto& expression : getNewMatchedExpressions(emptySubgraph, newSubgraph, whereClause)) {
            appendFilter(expression, *plan);
        }
        subPlansTable->addPlan(newSubgraph, move(plan));
    }
}

void Enumerator::enumerateNextLevel(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND) {
    currentLevel++;
    enumerateExtend(whereClauseSplitOnAND);
    // TODO: remove currentLevel constraint for Hash Join enumeration
    if (currentLevel >= 4) {
        enumerateHashJoin(whereClauseSplitOnAND);
    }
}

void Enumerator::enumerateExtend(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND) {
    auto& subgraphPlansMap = subPlansTable->getSubqueryGraphPlansMap(currentLevel - 1);
    for (auto& [prevSubgraph, prevPlans] : subgraphPlansMap) {
        auto connectedQueryRelsWithDirection =
            mergedQueryGraph->getConnectedQueryRelsWithDirection(prevSubgraph);
        for (auto& [relPos, isSrcConnected, isDstConnected] : connectedQueryRelsWithDirection) {
            if (isSrcConnected && isDstConnected) {
                throw invalid_argument("Intersect-like operator is not supported.");
            }
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
            for (auto& prevPlan : prevPlans) {
                auto newSubgraph = prevSubgraph;
                newSubgraph.addQueryRel(relPos);
                auto plan = prevPlan->copy();
                appendExtend(relPos, isSrcConnected ? FWD : BWD, *plan);
                for (auto& expression :
                    getNewMatchedExpressions(prevSubgraph, newSubgraph, whereClauseSplitOnAND)) {
                    appendFilter(expression, *plan);
                }
                subPlansTable->addPlan(newSubgraph, move(plan));
            }
        }
    }
}

void Enumerator::enumerateHashJoin(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND) {
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
                    leftSubgraph, rightSubgraph, newSubgraph, whereClauseSplitOnAND);
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
    plan.appendOperator(loadCSV);
}

void Enumerator::appendScan(uint32_t queryNodePos, LogicalPlan& plan) {
    auto& queryNode = *mergedQueryGraph->queryNodes[queryNodePos];
    auto nodeID = queryNode.getIDProperty();
    auto scan = plan.lastOperator ?
                    make_shared<LogicalScanNodeID>(nodeID, queryNode.label, plan.lastOperator) :
                    make_shared<LogicalScanNodeID>(nodeID, queryNode.label);
    auto groupPos = plan.schema->createGroup();
    plan.schema->appendToGroup(nodeID, groupPos);
    plan.schema->groups[groupPos].estimatedCardinality = graph.getNumNodes(queryNode.label);
    plan.appendOperator(scan);
}

void Enumerator::appendExtend(uint32_t queryRelPos, Direction direction, LogicalPlan& plan) {
    auto& queryRel = *mergedQueryGraph->queryRels[queryRelPos];
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
        appendFlatten(boundNodeID, plan);
        groupPos = plan.schema->createGroup();
        plan.schema->groups[groupPos].estimatedCardinality =
            plan.schema->groups[plan.schema->getGroupPos(boundNodeID)].estimatedCardinality *
            getExtensionRate(boundNode->label, queryRel.label, direction, graph);
    }
    auto extend = make_shared<LogicalExtend>(boundNodeID, boundNode->label, nbrNodeID,
        nbrNode->label, queryRel.label, direction, isColumnExtend, queryRel.lowerBound,
        queryRel.upperBound, plan.lastOperator);
    plan.schema->addLogicalExtend(queryRel.getInternalName(), extend.get());
    plan.schema->appendToGroup(nbrNodeID, groupPos);
    plan.appendOperator(extend);
}

string Enumerator::appendNecessaryFlattens(const Expression& expression,
    const unordered_map<uint32_t, string>& unFlatGroupsPos, LogicalPlan& plan) {
    string variableToSelect;
    if (unFlatGroupsPos.empty()) {
        // randomly pick a flat group
        auto subExpressions = expression.getIncludedExpressions(
            unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT, ALIAS});
        GF_ASSERT(!subExpressions.empty());
        variableToSelect = subExpressions[0]->getInternalName();
    } else {
        // randomly pick the first unFlat group to select and flatten the rest
        variableToSelect = unFlatGroupsPos.begin()->second;
        for (auto& [groupPos, variable] : unFlatGroupsPos) {
            if (variableToSelect != variable) {
                appendFlatten(variable, plan);
            }
        }
    }
    return variableToSelect;
}

void Enumerator::appendFlatten(const string& variable, LogicalPlan& plan) {
    auto groupPos = plan.schema->getGroupPos(variable);
    if (plan.schema->groups[groupPos].isFlat) {
        return;
    }
    plan.schema->flattenGroup(groupPos);
    plan.cost += plan.schema->groups[groupPos].estimatedCardinality;
    auto flatten = make_shared<LogicalFlatten>(variable, plan.lastOperator);
    plan.appendOperator(flatten);
}

void Enumerator::appendLogicalHashJoin(
    uint32_t joinNodePos, LogicalPlan& buildPlan, LogicalPlan& probePlan) {
    auto joinNodeID = mergedQueryGraph->queryNodes[joinNodePos]->getIDProperty();
    // Flat probe side key group if necessary
    auto probeSideKeyGroupPos = probePlan.schema->getGroupPos(joinNodeID);
    auto& probeSideKeyGroup = probePlan.schema->groups[probeSideKeyGroupPos];
    if (!probeSideKeyGroup.isFlat) {
        appendFlatten(joinNodeID, probePlan);
    }
    // Build side: 1) append non-key vectors in the key data chunk into probe side key data chunk.
    auto buildSideKeyGroupPos = buildPlan.schema->getGroupPos(joinNodeID);
    auto& buildSideKeyGroup = buildPlan.schema->groups[buildSideKeyGroupPos];
    for (auto& variable : buildSideKeyGroup.variables) {
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
        if (buildSideGroup.isFlat) {
            // 2) append flat non-key data chunks into buildSideNonKeyFlatDataChunk.
            if (probeSideAppendedFlatGroupPos == UINT32_MAX) {
                probeSideAppendedFlatGroupPos = probePlan.schema->createGroup();
            }
            probePlan.schema->appendToGroup(buildSideGroup, probeSideAppendedFlatGroupPos);
        } else {
            // 3) append unflat non-key data chunks as new data chunks into dataChunks.
            allGroupsOnBuildSideIsFlat = false;
            auto groupPos = probePlan.schema->createGroup();
            probePlan.schema->appendToGroup(buildSideGroup, groupPos);
            probePlan.schema->groups[groupPos].estimatedCardinality =
                buildSideGroup.estimatedCardinality;
        }
    }

    if (!allGroupsOnBuildSideIsFlat && probeSideAppendedFlatGroupPos != UINT32_MAX) {
        probePlan.schema->flattenGroup(probeSideAppendedFlatGroupPos);
    }
    auto hashJoin = make_shared<LogicalHashJoin>(
        joinNodeID, buildPlan.lastOperator, buildPlan.schema->copy(), probePlan.lastOperator);
    probeSideKeyGroup.estimatedCardinality =
        max(probeSideKeyGroup.estimatedCardinality, buildSideKeyGroup.estimatedCardinality);
    probePlan.appendOperator(hashJoin);
}

void Enumerator::appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan) {
    appendNecessaryScans(*expression, plan);
    auto unFlatGroupsPos = getUnFlatGroupsPos(expression, *plan.schema);
    auto variableToSelect = appendNecessaryFlattens(*expression, unFlatGroupsPos, plan);
    auto filter = make_shared<LogicalFilter>(expression, variableToSelect, plan.lastOperator);
    plan.schema->groups[plan.schema->getGroupPos(variableToSelect)].estimatedCardinality *=
        PREDICATE_SELECTIVITY;
    plan.appendOperator(filter);
}

void Enumerator::appendProjection(
    const vector<shared_ptr<Expression>>& expressions, LogicalPlan& plan) {
    // Do not append projection in case of RETURN COUNT(*)
    if (1 == expressions.size() && FUNCTION == expressions[0]->expressionType &&
        COUNT_STAR_FUNC_NAME ==
            static_pointer_cast<FunctionExpression>(expressions[0])->function.name) {
        return;
    }
    auto expressionsToProject = rewriteExpressionToProject(expressions, graph.getCatalog(), true);

    vector<uint32_t> exprResultInGroupPos;
    for (auto& expression : expressionsToProject) {
        appendNecessaryScans(*expression, plan);
        auto unFlatGroupsPos = getUnFlatGroupsPos(expression, *plan.schema);
        auto variableToWrite = appendNecessaryFlattens(*expression, unFlatGroupsPos, plan);
        exprResultInGroupPos.push_back(plan.schema->getGroupPos(variableToWrite));
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
            newSchema->groups[groupPos].isFlat =
                plan.schema->groups[exprResultInGroupPos[i]].isFlat;
            newSchema->groups[groupPos].estimatedCardinality =
                plan.schema->groups[exprResultInGroupPos[i]].estimatedCardinality;
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
    plan.appendOperator(projection);
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
    plan.appendOperator(scanProperty);
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
    plan.appendOperator(scanProperty);
}

uint64_t getExtensionRate(
    label_t boundNodeLabel, label_t relLabel, Direction direction, const Graph& graph) {
    auto numRels = graph.getNumRelsForDirBoundLabelRelLabel(direction, boundNodeLabel, relLabel);
    return ceil((double)numRels / graph.getNumNodes(boundNodeLabel));
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

unordered_map<uint32_t, string> getUnFlatGroupsPos(
    shared_ptr<Expression> expression, Schema& schema) {
    auto subExpressions = expression->getIncludedExpressions(
        unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT, ALIAS});
    unordered_map<uint32_t, string> unFlatGroupsPos;
    for (auto& subExpression : subExpressions) {
        if (schema.containVariable(subExpression->getInternalName())) {
            auto groupPos = schema.getGroupPos(subExpression->getInternalName());
            if (!schema.groups[groupPos].isFlat) {
                unFlatGroupsPos.insert({groupPos, subExpression->getInternalName()});
            }
        } else {
            GF_ASSERT(ALIAS == subExpression->expressionType);
            for (auto& [groupPos, variable] :
                getUnFlatGroupsPos(subExpression->children[0], schema)) {
                unFlatGroupsPos.insert({groupPos, variable});
            }
        }
    }
    return unFlatGroupsPos;
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
