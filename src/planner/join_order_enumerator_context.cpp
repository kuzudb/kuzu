#include "src/planner/include/join_order_enumerator_context.h"

namespace graphflow {
namespace planner {

void JoinOrderEnumeratorContext::init(const QueryGraph& queryGraph,
    const shared_ptr<Expression>& queryGraphPredicate, vector<unique_ptr<LogicalPlan>> prevPlans) {
    // split where expression
    whereExpressionsSplitOnAND =
        queryGraphPredicate ? queryGraphPredicate->splitOnAND() : vector<shared_ptr<Expression>>();
    // merge new query graph
    auto fullyMatchedSubqueryGraph = getFullyMatchedSubqueryGraph();
    matchedQueryRels = fullyMatchedSubqueryGraph.queryRelsSelector;
    matchedQueryNodes = fullyMatchedSubqueryGraph.queryNodesSelector;
    mergeQueryGraph(queryGraph);
    // clear and resize subPlansTable
    subPlansTable->clear();
    subPlansTable->resize(mergedQueryGraph->getNumQueryRels());
    // add plans from previous query part into subPlanTable
    for (auto& prevPlan : prevPlans) {
        subPlansTable->addPlan(fullyMatchedSubqueryGraph, move(prevPlan));
    }
    // Restart from level 0 for new query part so that we get hashJoin based plans
    // that uses subplans coming from previous query part.See example in enumerateExtend().
    currentLevel = 0;
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

} // namespace planner
} // namespace graphflow
