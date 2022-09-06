#include "src/planner/include/join_order_enumerator_context.h"

namespace graphflow {
namespace planner {

void JoinOrderEnumeratorContext::init(const QueryGraph& queryGraph,
    const shared_ptr<Expression>& queryGraphPredicate, vector<unique_ptr<LogicalPlan>> prevPlans) {
    whereExpressionsSplitOnAND =
        queryGraphPredicate ? queryGraphPredicate->splitOnAND() : expression_vector();
    // merge new query graph
    auto fullyMatchedSubqueryGraph = getFullyMatchedSubqueryGraph();
    mergeQueryGraph(queryGraph);
    // clear and resize subPlansTable
    subPlansTable->clear();
    maxLevel = mergedQueryGraph->getNumQueryNodes() + mergedQueryGraph->getNumQueryRels() + 1;
    subPlansTable->resize(maxLevel);
    // add plans from previous query part into subPlanTable
    for (auto& prevPlan : prevPlans) {
        subPlansTable->addPlan(fullyMatchedSubqueryGraph, move(prevPlan));
    }
    // Restart from level 1 for new query part so that we get hashJoin based plans
    // that uses subplans coming from previous query part.See example in planRelIndexJoin().
    currentLevel = 1;
}

SubqueryGraph JoinOrderEnumeratorContext::getFullyMatchedSubqueryGraph() const {
    auto matchedSubgraph = SubqueryGraph(*mergedQueryGraph);
    for (auto i = 0u; i < mergedQueryGraph->getNumQueryNodes(); ++i) {
        matchedSubgraph.addQueryNode(i);
    }
    for (auto i = 0u; i < mergedQueryGraph->getNumQueryRels(); ++i) {
        matchedSubgraph.addQueryRel(i);
    }
    return matchedSubgraph;
}

void JoinOrderEnumeratorContext::resetState() {
    subPlansTable = make_unique<SubPlansTable>();
    mergedQueryGraph = make_unique<QueryGraph>();
}

} // namespace planner
} // namespace graphflow
