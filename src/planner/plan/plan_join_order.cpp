#include "binder/expression_visitor.h"
#include "common/enums/join_type.h"
#include "planner/join_order/cost_model.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/planner.h"
#include "planner/join_order/join_order_solver.h"
#include "planner/join_order/join_plan_solver.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planQueryGraphCollection(
    const QueryGraphCollection& queryGraphCollection, const expression_vector& predicates) {
    return getBestPlan(enumerateQueryGraphCollection(queryGraphCollection, predicates));
}

std::unique_ptr<LogicalPlan> Planner::planQueryGraphCollectionInNewContext(
    SubqueryType subqueryType, const expression_vector& correlatedExpressions, uint64_t cardinality,
    const QueryGraphCollection& queryGraphCollection, const expression_vector& predicates) {
    auto plans = enumerateQueryGraphCollection(queryGraphCollection, predicates);
    return getBestPlan(std::move(plans));
}

static int32_t getConnectedQueryGraphIdx(const QueryGraphCollection& queryGraphCollection,
    const expression_set& expressionSet) {
    for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection.getQueryGraph(i);
        for (auto& queryNode : queryGraph->getQueryNodes()) {
            if (expressionSet.contains(queryNode->getInternalID())) {
                return i;
            }
        }
    }
    return -1;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::enumerateQueryGraphCollection(
    const SubqueryPlanInfo& subqueryPlanInfo, const QueryGraphCollection& queryGraphCollection,
    const expression_vector& predicates) {
    KU_ASSERT(queryGraphCollection.getNumQueryGraphs() > 0);
    auto corrExprsSet = expression_set{subqueryPlanInfo.corrExprs.begin(), subqueryPlanInfo.corrExprs.end()};
    int32_t queryGraphIdxToPlanExpressionsScan = -1;
    if (subqueryPlanInfo.subqueryType == SubqueryType::CORRELATED) {
        // Pick a query graph to plan ExpressionsScan. If -1 is returned, we fall back to cross
        // product.
        queryGraphIdxToPlanExpressionsScan =
            getConnectedQueryGraphIdx(queryGraphCollection, corrExprsSet);
    }
    std::unordered_set<uint32_t> evaluatedPredicatesIndices;
    std::vector<std::vector<std::unique_ptr<LogicalPlan>>> plansPerQueryGraph;
    for (auto i = 0u; i < queryGraphCollection.getNumQueryGraphs(); ++i) {
        auto queryGraph = queryGraphCollection.getQueryGraph(i);
        // Extract predicates for current query graph
        std::unordered_set<uint32_t> predicateToEvaluateIndices;
        for (auto j = 0u; j < predicates.size(); ++j) {
            if (predicates[j]->expressionType == ExpressionType::LITERAL) {
                continue;
            }
            if (evaluatedPredicatesIndices.contains(j)) {
                continue;
            }
            if (queryGraph->canProjectExpression(predicates[j])) {
                predicateToEvaluateIndices.insert(j);
            }
        }
        evaluatedPredicatesIndices.insert(predicateToEvaluateIndices.begin(),
            predicateToEvaluateIndices.end());
        expression_vector predicatesToEvaluate;
        for (auto idx : predicateToEvaluateIndices) {
            predicatesToEvaluate.push_back(predicates[idx]);
        }
        std::vector<std::unique_ptr<LogicalPlan>> plans;
        switch (subqueryPlanInfo.subqueryType) {
        case SubqueryType::NONE: {
            // Plan current query graph as an isolated query graph.
            plans = enumerateQueryGraph(SubqueryType::NONE, expression_vector{}, *queryGraph,
                predicatesToEvaluate);
        } break;
        case SubqueryType::INTERNAL_ID_CORRELATED: {
            // All correlated expressions are node IDs. Plan as isolated query graph but do not scan
            // any properties of correlated node IDs because they must be scanned in outer query.
            plans = enumerateQueryGraph(SubqueryType::INTERNAL_ID_CORRELATED,
                context.correlatedExpressions, *queryGraph, predicatesToEvaluate);
        } break;
        case SubqueryType::CORRELATED: {
            if (i == (uint32_t)queryGraphIdxToPlanExpressionsScan) {
                // Plan ExpressionsScan with current query graph.
                plans = enumerateQueryGraph(SubqueryType::CORRELATED, context.correlatedExpressions,
                    *queryGraph, predicatesToEvaluate);
            } else {
                // Plan current query graph as an isolated query graph.
                plans = enumerateQueryGraph(SubqueryType::NONE, expression_vector{}, *queryGraph,
                    predicatesToEvaluate);
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
        plansPerQueryGraph.push_back(std::move(plans));
    }
    // Fail to plan ExpressionsScan with any query graph. Plan it independently and fall back to
    // cross product.
    if (subqueryPlanInfo.subqueryType == SubqueryType::CORRELATED &&
        queryGraphIdxToPlanExpressionsScan == -1) {
        auto plan = std::make_unique<LogicalPlan>();
        appendExpressionsScan(context.getCorrelatedExpressions(), *plan);
        appendDistinct(context.getCorrelatedExpressions(), *plan);
        std::vector<std::unique_ptr<LogicalPlan>> plans;
        plans.push_back(std::move(plan));
        plansPerQueryGraph.push_back(std::move(plans));
    }
    // Take cross products
    auto result = std::move(plansPerQueryGraph[0]);
    for (auto i = 1u; i < plansPerQueryGraph.size(); ++i) {
        result = planCrossProduct(std::move(result), std::move(plansPerQueryGraph[i]));
    }
    // Apply remaining predicates
    expression_vector remainingPredicates;
    for (auto i = 0u; i < predicates.size(); ++i) {
        if (!evaluatedPredicatesIndices.contains(i)) {
            remainingPredicates.push_back(predicates[i]);
        }
    }
    for (auto& plan : result) {
        for (auto& predicate : remainingPredicates) {
            appendFilter(predicate, *plan);
        }
    }
    return result;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::enumerateQueryGraph(
    const SubqueryPlanInfo& subqueryPlanInfo, const QueryGraph& queryGraph,
    expression_vector& predicates) {
    cardinalityEstimator.initNodeIDDom(queryGraph, clientContext->getTx());
    // Init properties to scan current query graph.
    PropertyExprCollection propertyExprCollection;
    switch (subqueryPlanInfo.subqueryType) {
    case SubqueryType::NONE: {
        for (auto& node : queryGraph.getQueryNodes()) {
            auto properties = getProperties(*node);
            propertyExprCollection.addProperties(node, properties);
        }
    } break ;
    case SubqueryType::INTERNAL_ID_CORRELATED:
    case SubqueryType::CORRELATED: {
        auto& corrExprs = subqueryPlanInfo.corrExprs;
        auto set = expression_set{corrExprs.begin(), corrExprs.end()};
        for (auto& node : queryGraph.getQueryNodes()) {
            if (set.contains(node->getInternalID())) {
                continue ;
            }
            auto nodeProperties = getProperties(*node);
            propertyExprCollection.addProperties(node, nodeProperties);
        }
    } break ;
    default:
        KU_UNREACHABLE;
    }
    for (auto& rel : queryGraph.getQueryRels()) {
        if (ExpressionUtil::isRecursiveRelPattern(*rel)) {
            continue ;
        }
        auto properties = getProperties(*rel);
        propertyExprCollection.addProperties(rel, properties);
    }
    auto joinOrderSolver = JoinOrderSolver(queryGraph, predicates, std::move(propertyExprCollection), clientContext);
    // Init correlated expressions.
    switch (subqueryPlanInfo.subqueryType) {
    case SubqueryType::INTERNAL_ID_CORRELATED:
    case SubqueryType::CORRELATED: {
        // TODO: we shouldn't get correlated expr cardinality from context.
        joinOrderSolver.setCorrExprs(subqueryType, corrExprs, context.correlatedExpressionsCardinality);
    } break ;
    default:
        break;
    }

    auto joinTree = joinOrderSolver.solve();
    auto joinPlanSolver = JoinPlanSolver(this);
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    auto plan = joinPlanSolver.solve(joinTree);
    if (queryGraph.isEmpty()) {
        appendEmptyResult(plan);
    }
    auto s = plan.toString();
    plans.push_back(plan.shallowCopy());
    return plans;
}

//void Planner::planLevelApproximately(uint32_t level) {
//    planInnerJoin(1, level - 1);
//}

static bool isExpressionNewlyMatched(const std::vector<SubqueryGraph>& prevSubgraphs,
    const SubqueryGraph& newSubgraph, const std::shared_ptr<Expression>& expression) {
    auto expressionCollector = std::make_unique<ExpressionCollector>();
    auto variables = expressionCollector->getDependentVariableNames(expression);
    for (auto& prevSubgraph : prevSubgraphs) {
        if (prevSubgraph.containAllVariables(variables)) {
            return false; // matched in prev subgraph
        }
    }
    return newSubgraph.containAllVariables(variables);
}

expression_vector Planner::getNewlyMatchedExpressions(const std::vector<SubqueryGraph>& prevSubgraphs,
    const SubqueryGraph& newSubgraph, const expression_vector& expressions) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (isExpressionNewlyMatched(prevSubgraphs, newSubgraph, expression)) {
            result.push_back(expression);
        }
    }
    return result;
}

binder::expression_vector Planner::getNewlyMatchedExpressions(const SubqueryGraph& leftPrev,
    const SubqueryGraph& rightPrev, const SubqueryGraph& newSubgraph,
    const expression_vector& expressions) {
    return getNewlyMatchedExpressions(std::vector<SubqueryGraph>{leftPrev, rightPrev}, newSubgraph,
        expressions);
}

expression_vector Planner::getNewlyMatchedExpressions(const SubqueryGraph& prevSubgraph,
    const SubqueryGraph& newSubgraph, const expression_vector& expressions) {
    return getNewlyMatchedExpressions(std::vector<SubqueryGraph>{prevSubgraph}, newSubgraph,
        expressions);
}

void Planner::appendExtend(std::shared_ptr<NodeExpression> boundNode,
    std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
    ExtendDirection direction, const binder::expression_vector& properties, LogicalPlan& plan) {
    switch (rel->getRelType()) {
    case QueryRelType::NON_RECURSIVE: {
        appendNonRecursiveExtend(boundNode, nbrNode, rel, direction, properties, plan);
    } break;
    case QueryRelType::VARIABLE_LENGTH:
    case QueryRelType::SHORTEST:
    case QueryRelType::ALL_SHORTEST: {
        appendRecursiveExtend(boundNode, nbrNode, rel, direction, plan);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::planCrossProduct(
    std::vector<std::unique_ptr<LogicalPlan>> leftPlans,
    std::vector<std::unique_ptr<LogicalPlan>> rightPlans) {
    std::vector<std::unique_ptr<LogicalPlan>> result;
    for (auto& leftPlan : leftPlans) {
        for (auto& rightPlan : rightPlans) {
            auto leftPlanCopy = leftPlan->shallowCopy();
            auto rightPlanCopy = rightPlan->shallowCopy();
            appendCrossProduct(AccumulateType::REGULAR, *leftPlanCopy, *rightPlanCopy,
                *leftPlanCopy);
            result.push_back(std::move(leftPlanCopy));
        }
    }
    return result;
}

} // namespace planner
} // namespace kuzu
