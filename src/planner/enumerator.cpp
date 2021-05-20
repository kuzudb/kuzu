#include "src/planner/include/enumerator.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/load_csv/logical_load_csv.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"

const double PREDICATE_SELECTIVITY = 0.2;

namespace graphflow {
namespace planner {

// return empty string if all variables are flat
static string getLargestUnflatVariableAndFlattenOthers(
    const Expression& expression, LogicalPlan& plan);

static uint64_t getExtensionRate(
    label_t boundNodeLabel, label_t relLabel, Direction direction, const Graph& graph);

static vector<shared_ptr<Expression>> getNewMatchedWhereExpressions(
    const SubqueryGraph& prevSubgraph, const SubqueryGraph& newSubgraph,
    const vector<shared_ptr<Expression>>& expressions);

static vector<shared_ptr<Expression>> getNewMatchedWhereExpressions(
    const SubqueryGraph& prevLeftSubgraph, const SubqueryGraph& prevRightSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions);

static vector<shared_ptr<Expression>> splitExpressionOnAND(shared_ptr<Expression> expression);

static vector<shared_ptr<Expression>> rewriteVariableAsAllProperties(
    shared_ptr<Expression> variableExpression, const Catalog& catalog);

static vector<shared_ptr<Expression>> createLogicalPropertyExpressions(
    const string& variableName, const unordered_map<string, PropertyKey>& propertyMap);

Enumerator::Enumerator(const Graph& graph, const BoundSingleQuery& boundSingleQuery)
    : graph{graph}, boundSingleQuery{boundSingleQuery} {
    subplansTable = make_unique<SubplansTable>(boundSingleQuery.getNumQueryRels());
    mergedQueryGraph = make_unique<QueryGraph>();
}

unique_ptr<LogicalPlan> Enumerator::getBestPlan() {
    auto plans = enumeratePlans();
    unique_ptr<LogicalPlan> bestPlan = move(plans[0]);
    for (auto i = 1u; i < plans.size(); ++i) {
        if (plans[i]->cost < bestPlan->cost) {
            bestPlan = move(plans[i]);
        }
    }
    return bestPlan;
}

vector<unique_ptr<LogicalPlan>> Enumerator::enumeratePlans() {
    for (auto& boundQueryPart : boundSingleQuery.boundQueryParts) {
        enumerateBoundQueryPart(*boundQueryPart);
    }
    auto whereClauseSplitOnAND = vector<shared_ptr<Expression>>();
    for (auto i = 0u; i < boundSingleQuery.boundReadingStatements.size(); ++i) {
        auto& boundReadingStatement = *boundSingleQuery.boundReadingStatements[i];
        if (LOAD_CSV_STATEMENT == boundReadingStatement.statementType) {
            throw invalid_argument("should not happen.");
        } else {
            assert(i == boundSingleQuery.boundReadingStatements.size() - 1);
            updateQueryGraphAndWhereClause(
                (BoundMatchStatement&)boundReadingStatement, whereClauseSplitOnAND);
        }
    }
    enumerateSubplans(whereClauseSplitOnAND, boundSingleQuery.boundReturnStatement->expressions);
    return move(subplansTable->subgraphPlans[currentLevel].begin()->second);
}

void Enumerator::enumerateBoundQueryPart(BoundQueryPart& boundQueryPart) {
    auto whereClauseSplitOnAND = vector<shared_ptr<Expression>>();
    for (auto i = 0u; i < boundQueryPart.boundReadingStatements.size(); ++i) {
        auto& boundReadingStatement = *boundQueryPart.boundReadingStatements[i];
        if (LOAD_CSV_STATEMENT == boundReadingStatement.statementType) {
            throw invalid_argument("should not happen.");
        } else {
            assert(i == boundQueryPart.boundReadingStatements.size() - 1);
            updateQueryGraphAndWhereClause(
                (BoundMatchStatement&)boundReadingStatement, whereClauseSplitOnAND);
        }
    }
    if (boundQueryPart.boundWithStatement->whereExpression) {
        for (auto& expression :
            splitExpressionOnAND(boundQueryPart.boundWithStatement->whereExpression)) {
            whereClauseSplitOnAND.push_back(expression);
        }
    }
    enumerateSubplans(whereClauseSplitOnAND, boundQueryPart.boundWithStatement->expressions);
}

void Enumerator::updateQueryGraphAndWhereClause(BoundMatchStatement& boundMatchStatement,
    vector<shared_ptr<Expression>>& whereClauseSplitOnAND) {
    if (!mergedQueryGraph->isEmpty()) {
        // When entering from one query part to another, subgraphPlans at currentLevel
        // contains only one entry which is the full mergedQueryGraph
        assert(1 == subplansTable->subgraphPlans[currentLevel].size());
        subplansTable->clearUntil(currentLevel);
        matchedQueryRels =
            subplansTable->subgraphPlans[currentLevel].begin()->first.queryRelsSelector;
    }
    mergedQueryGraph->merge(*boundMatchStatement.queryGraph);
    // Restart from level 0 for new query part so that we get hashJoin based plans
    // that uses subplans coming from previous query part.
    // See example in enumerateExtend().
    currentLevel = 0;
    if (boundMatchStatement.whereExpression) {
        for (auto& expression : splitExpressionOnAND(boundMatchStatement.whereExpression)) {
            whereClauseSplitOnAND.push_back(expression);
        }
    }
}

void Enumerator::enumerateSubplans(const vector<shared_ptr<Expression>>& whereClause,
    const vector<shared_ptr<Expression>>& returnOrWithClause) {
    // first query part may not have query graph
    // E.g. WITH 1 AS one MATCH (a) ...
    if (mergedQueryGraph->isEmpty()) {
        return;
    }
    enumerateSingleQueryNode(whereClause);
    while (currentLevel < mergedQueryGraph->getNumQueryRels()) {
        enumerateNextLevel(whereClause);
    }
    assert(1 == subplansTable->subgraphPlans[currentLevel].size());
    if (auto APPEND_PROJECTION = false) {
        auto& plans = subplansTable->subgraphPlans[currentLevel].begin()->second;
        for (auto& plan : plans) {
            appendProjection(returnOrWithClause, *plan);
        }
    }
}

void Enumerator::enumerateSingleQueryNode(
    const vector<shared_ptr<Expression>>& whereClauseSplitOnAND) {
    for (auto nodePos = 0u; nodePos < mergedQueryGraph->getNumQueryNodes(); ++nodePos) {
        auto newSubgraph = SubqueryGraph(*mergedQueryGraph);
        newSubgraph.addQueryNode(nodePos);
        auto plan = make_unique<LogicalPlan>();
        appendLogicalScan(nodePos, *plan);
        for (auto& expression :
            getNewMatchedWhereExpressions(SubqueryGraph(*mergedQueryGraph) /* empty subGraph */,
                newSubgraph, whereClauseSplitOnAND)) {
            appendFilter(expression, *plan);
        }
        subplansTable->addSubgraphPlan(newSubgraph, move(plan));
    }
}

void Enumerator::enumerateNextLevel(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND) {
    currentLevel++;
    enumerateExtend(whereClauseSplitOnAND);
    if (currentLevel >= 4) {
        enumerateHashJoin(whereClauseSplitOnAND);
    }
}

void Enumerator::enumerateExtend(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND) {
    auto& subgraphPlansMap = subplansTable->subgraphPlans[currentLevel - 1];
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
            if (isSrcConnected && isDstConnected) {
                throw invalid_argument("Intersect-like operator is not supported.");
            }
            for (auto& prevPlan : prevPlans) {
                auto newSubgraph = prevSubgraph;
                newSubgraph.addQueryRel(relPos);
                auto plan = prevPlan->copy();
                appendLogicalExtend(relPos, isSrcConnected ? FWD : BWD, *plan);
                for (auto& expression : getNewMatchedWhereExpressions(
                         prevSubgraph, newSubgraph, whereClauseSplitOnAND)) {
                    appendFilter(expression, *plan);
                }
                subplansTable->addSubgraphPlan(newSubgraph, move(plan));
            }
        }
    }
}

void Enumerator::enumerateHashJoin(const vector<shared_ptr<Expression>>& whereClauseSplitOnAND) {
    for (auto leftSize = currentLevel - 2; leftSize >= ceil(currentLevel / 2.0); --leftSize) {
        auto& subgraphPlansMap = subplansTable->subgraphPlans[leftSize];
        for (auto& [leftSubgraph, leftPlans] : subgraphPlansMap) {
            auto rightSubgraphAndJoinNodePairs = mergedQueryGraph->getSingleNodeJoiningSubgraph(
                leftSubgraph, currentLevel - leftSize);
            for (auto& [rightSubgraph, joinNodePos] : rightSubgraphAndJoinNodePairs) {
                // Consider previous example in enumerateExtend()
                // When enumerating second MATCH, and current level = 4
                // we get left subgraph as f, d, e (size = 2), and try to find a connected
                // right subgraph of size 2. A possible right graph could be b, c, d.
                // However, b, c, d is a subgraph enumerated in the first MATCH and has been
                // cleared before enumeration of second MATCH. So subplansTable does not
                // contain this subgraph.
                if (!subplansTable->containSubgraphPlans(rightSubgraph)) {
                    continue;
                }
                auto& rightPlans = subplansTable->getSubgraphPlans(rightSubgraph);
                auto newSubgraph = leftSubgraph;
                newSubgraph.addSubqueryGraph(rightSubgraph);
                auto expressionsToFilter = getNewMatchedWhereExpressions(
                    leftSubgraph, rightSubgraph, newSubgraph, whereClauseSplitOnAND);
                for (auto& leftPlan : leftPlans) {
                    for (auto& rightPlan : rightPlans) {
                        auto plan = leftPlan->copy();
                        appendLogicalHashJoin(joinNodePos, *rightPlan, *plan);
                        for (auto& expression : expressionsToFilter) {
                            appendFilter(expression, *plan);
                        }
                        subplansTable->addSubgraphPlan(newSubgraph, move(plan));
                        // flip build and probe side to get another HashJoin plan
                        if (leftSize != currentLevel - leftSize) {
                            auto planFilpped = rightPlan->copy();
                            appendLogicalHashJoin(joinNodePos, *leftPlan, *planFilpped);
                            for (auto& expression : expressionsToFilter) {
                                appendFilter(expression, *planFilpped);
                            }
                            subplansTable->addSubgraphPlan(newSubgraph, move(planFilpped));
                        }
                    }
                }
            }
        }
    }
}

void Enumerator::appendLogicalScan(uint32_t queryNodePos, LogicalPlan& plan) {
    auto& queryNode = *mergedQueryGraph->queryNodes[queryNodePos];
    if (ANY_LABEL == queryNode.label) {
        throw invalid_argument("Match any label is not yet supported in LogicalScanNodeID.");
    }
    auto nodeID = queryNode.getIDProperty();
    auto scan = make_shared<LogicalScanNodeID>(nodeID, queryNode.label);
    plan.schema->addMatchedAttribute(nodeID);
    plan.schema->initFlatFactorizationGroup(
        queryNode.variableName, graph.getNumNodes(queryNode.label));
    plan.appendOperator(scan);
}

void Enumerator::appendLogicalExtend(uint32_t queryRelPos, Direction direction, LogicalPlan& plan) {
    auto& queryRel = *mergedQueryGraph->queryRels[queryRelPos];
    if (ANY_LABEL == queryRel.srcNode->label && ANY_LABEL == queryRel.dstNode->label &&
        ANY_LABEL == queryRel.label) {
        throw invalid_argument("Match any label is not yet supported in LogicalExtend");
    }
    auto boundNode = FWD == direction ? queryRel.srcNode : queryRel.dstNode;
    auto nbrNode = FWD == direction ? queryRel.dstNode : queryRel.srcNode;
    auto boundNodeID = boundNode->getIDProperty();
    auto nbrNodeID = nbrNode->getIDProperty();
    auto isColumnExtend = graph.getCatalog().isSingleCaridinalityInDir(queryRel.label, direction);
    auto extend = make_shared<LogicalExtend>(boundNodeID, boundNode->label, nbrNodeID,
        nbrNode->label, queryRel.label, direction, isColumnExtend, plan.lastOperator);
    plan.schema->addMatchedAttribute(nbrNodeID);
    plan.schema->addMatchedAttribute(queryRel.variableName);
    plan.schema->addQueryRelAndLogicalExtend(queryRel.variableName, extend.get());
    if (isColumnExtend) {
        plan.schema->getFactorizationGroup(boundNode->variableName)
            ->addVariables(unordered_set<string>{queryRel.variableName, nbrNode->variableName});
    } else {
        plan.schema->flattenFactorizationGroupIfNecessary(boundNode->variableName);
        plan.schema->addUnFlatFactorizationGroup(
            unordered_set<string>{queryRel.variableName, nbrNode->variableName},
            getExtensionRate(boundNode->label, queryRel.label, direction, graph));
    }
    plan.cost += plan.schema->getCardinality();
    plan.appendOperator(extend);
}

void Enumerator::appendLogicalHashJoin(
    uint32_t joinNodePos, const LogicalPlan& planToJoin, LogicalPlan& plan) {
    auto joinNodeID = mergedQueryGraph->queryNodes[joinNodePos]->getIDProperty();
    auto hashJoin =
        make_shared<LogicalHashJoin>(joinNodeID, plan.lastOperator, planToJoin.lastOperator);
    plan.schema->merge(*planToJoin.schema);
    plan.appendOperator(hashJoin);
}

void Enumerator::appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan) {
    appendNecessaryScans(expression, plan);
    auto filter = make_shared<LogicalFilter>(expression, plan.lastOperator);
    auto largestUnflatVariable = getLargestUnflatVariableAndFlattenOthers(*expression, plan);
    if (largestUnflatVariable.empty()) {
        plan.schema->flatGroup->cardinalityOrExtensionRate *= PREDICATE_SELECTIVITY;
    } else {
        plan.schema->getFactorizationGroup(largestUnflatVariable)->cardinalityOrExtensionRate *=
            PREDICATE_SELECTIVITY;
    }
    plan.appendOperator(filter);
}

void Enumerator::appendProjection(
    const vector<shared_ptr<Expression>>& returnOrWithClause, LogicalPlan& plan) {
    // Do not append projection in case of RETURN COUNT(*)
    if (1 == returnOrWithClause.size() && FUNCTION == returnOrWithClause[0]->expressionType &&
        FUNCTION_COUNT_STAR == returnOrWithClause[0]->variableName) {
        return;
    }
    auto expressionsToProject = vector<shared_ptr<Expression>>();
    for (auto& expression : returnOrWithClause) {
        if (VARIABLE == expression->expressionType) {
            for (auto& propertyExpression :
                rewriteVariableAsAllProperties(expression, graph.getCatalog())) {
                appendNecessaryScans(propertyExpression, plan);
                expressionsToProject.push_back(propertyExpression);
            }
        } else {
            appendNecessaryScans(expression, plan);
            expressionsToProject.push_back(expression);
            getLargestUnflatVariableAndFlattenOthers(*expression, plan);
        }
    }
    auto projection = make_shared<LogicalProjection>(move(expressionsToProject), plan.lastOperator);
    plan.appendOperator(projection);
}

void Enumerator::appendNecessaryScans(shared_ptr<Expression> expression, LogicalPlan& plan) {
    for (auto& propertyExpression : expression->getIncludedPropertyExpressions()) {
        if (plan.schema->containsAttributeName(propertyExpression->variableName)) {
            continue;
        }
        NODE == propertyExpression->childrenExpr[0]->dataType ?
            appendScanNodeProperty((PropertyExpression&)*propertyExpression, plan) :
            appendScanRelProperty((PropertyExpression&)*propertyExpression, plan);
    }
}

void Enumerator::appendScanNodeProperty(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto nodeExpression = static_pointer_cast<NodeExpression>(propertyExpression.childrenExpr[0]);
    auto scanProperty = make_shared<LogicalScanNodeProperty>(nodeExpression->getIDProperty(),
        nodeExpression->label, propertyExpression.variableName, propertyExpression.propertyKey,
        UNSTRUCTURED == propertyExpression.dataType, plan.lastOperator);
    plan.schema->addMatchedAttribute(propertyExpression.variableName);
    plan.appendOperator(scanProperty);
}

void Enumerator::appendScanRelProperty(
    const PropertyExpression& propertyExpression, LogicalPlan& plan) {
    auto relExpression = static_pointer_cast<RelExpression>(propertyExpression.childrenExpr[0]);
    auto extend = plan.schema->getExistingLogicalExtend(relExpression->variableName);
    auto scanProperty =
        make_shared<LogicalScanRelProperty>(extend->boundNodeID, extend->boundNodeLabel,
            extend->nbrNodeID, extend->relLabel, extend->direction, propertyExpression.variableName,
            propertyExpression.propertyKey, extend->isColumn, plan.lastOperator);
    plan.schema->addMatchedAttribute(propertyExpression.variableName);
    plan.appendOperator(scanProperty);
}

string getLargestUnflatVariableAndFlattenOthers(const Expression& expression, LogicalPlan& plan) {
    auto unflatVariables = vector<string>();
    for (auto& variable : expression.getIncludedVariableNames()) {
        if (!plan.schema->isVariableFlat(variable)) {
            unflatVariables.push_back(variable);
        }
    }
    auto largestUnflatVariable = string();
    auto largestExtensionRate = 0u;
    for (auto& unflatVariable : unflatVariables) {
        if (plan.schema->getFactorizationGroup(unflatVariable)->cardinalityOrExtensionRate >
            largestExtensionRate) {
            largestUnflatVariable = unflatVariable;
            largestExtensionRate =
                plan.schema->getFactorizationGroup(unflatVariable)->cardinalityOrExtensionRate;
        }
    }
    for (auto& unflatVariable : unflatVariables) {
        if (unflatVariable != largestUnflatVariable) {
            plan.schema->flattenFactorizationGroupIfNecessary(unflatVariable);
            plan.cost += plan.schema->getCardinality();
        }
    }
    return largestUnflatVariable;
}

uint64_t getExtensionRate(
    label_t boundNodeLabel, label_t relLabel, Direction direction, const Graph& graph) {
    auto numRels = graph.getNumRelsForDirBoundLabelRelLabel(direction, boundNodeLabel, relLabel);
    return ceil((double)numRels / graph.getNumNodes(boundNodeLabel));
}

vector<shared_ptr<Expression>> getNewMatchedWhereExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const vector<shared_ptr<Expression>>& expressions) {
    vector<shared_ptr<Expression>> newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getIncludedVariableNames();
        if (newSubgraph.containAllVars(includedVariables) &&
            !prevSubgraph.containAllVars(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

vector<shared_ptr<Expression>> getNewMatchedWhereExpressions(const SubqueryGraph& prevLeftSubgraph,
    const SubqueryGraph& prevRightSubgraph, const SubqueryGraph& newSubgraph,
    const vector<shared_ptr<Expression>>& expressions) {
    vector<shared_ptr<Expression>> newMatchedExpressions;
    for (auto& expression : expressions) {
        auto includedVariables = expression->getIncludedVariableNames();
        if (newSubgraph.containAllVars(includedVariables) &&
            !prevLeftSubgraph.containAllVars(includedVariables) &&
            !prevRightSubgraph.containAllVars(includedVariables)) {
            newMatchedExpressions.push_back(expression);
        }
    }
    return newMatchedExpressions;
}

vector<shared_ptr<Expression>> splitExpressionOnAND(shared_ptr<Expression> expression) {
    auto result = vector<shared_ptr<Expression>>();
    if (AND == expression->expressionType) {
        for (auto& child : expression->childrenExpr) {
            for (auto& exp : splitExpressionOnAND(child)) {
                result.push_back(exp);
            }
        }
    } else {
        result.push_back(expression);
    }
    return result;
}

// all properties are given an alias in order to print
static vector<shared_ptr<Expression>> rewriteVariableAsAllProperties(
    shared_ptr<Expression> variableExpression, const Catalog& catalog) {
    if (NODE == variableExpression->dataType) {
        auto nodeExpression = static_pointer_cast<NodeExpression>(variableExpression);
        auto propertyExpressions = createLogicalPropertyExpressions(nodeExpression->variableName,
            catalog.getPropertyKeyMapForNodeLabel(nodeExpression->label));
        // unstructured properties
        for (auto& propertyExpression :
            createLogicalPropertyExpressions(nodeExpression->variableName,
                catalog.getUnstrPropertyKeyMapForNodeLabel(nodeExpression->label))) {
            propertyExpressions.push_back(propertyExpression);
        }
        auto idProperty =
            make_shared<Expression>(PROPERTY, NODE_ID, nodeExpression->getIDProperty());
        idProperty->alias = nodeExpression->getIDProperty();
        propertyExpressions.push_back(idProperty);
        return propertyExpressions;
    } else {
        auto relExpression = static_pointer_cast<RelExpression>(variableExpression);
        return createLogicalPropertyExpressions(relExpression->variableName,
            catalog.getPropertyKeyMapForRelLabel(relExpression->label));
    }
}

vector<shared_ptr<Expression>> createLogicalPropertyExpressions(
    const string& variableName, const unordered_map<string, PropertyKey>& propertyMap) {
    auto propertyExpressions = vector<shared_ptr<Expression>>();
    for (auto& [propertyName, property] : propertyMap) {
        auto propertyWithVariableName = variableName + "." + propertyName;
        auto expression =
            make_shared<Expression>(PROPERTY, property.dataType, propertyWithVariableName);
        // This alias set should be removed if we can print all properties in a single column.
        // And column name should be variable name
        expression->alias = propertyWithVariableName;
        propertyExpressions.push_back(expression);
    }
    return propertyExpressions;
}

} // namespace planner
} // namespace graphflow
