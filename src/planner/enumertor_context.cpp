#include "src/planner/include/enumerator_context.h"

namespace graphflow {
namespace planner {

void EnumeratorContext::prepareQueryGraph(QueryGraph& queryGraph) {
    auto fullyMatchedSubqueryGraph = getFullyMatchedSubqueryGraph();
    matchedQueryRels = fullyMatchedSubqueryGraph.queryRelsSelector;
    matchedQueryNodes = fullyMatchedSubqueryGraph.queryNodesSelector;
    // Keep only plans in the last level and clean cached plans
    subPlansTable->clearUntil(currentLevel);
    mergedQueryGraph->merge(queryGraph);
    // Restart from level 0 for new query part so that we get hashJoin based plans
    // that uses subplans coming from previous query part.See example in enumerateExtend().
    currentLevel = 0;
}

SubqueryGraph EnumeratorContext::getEmptySubqueryGraph() const {
    return SubqueryGraph(*mergedQueryGraph);
}

SubqueryGraph EnumeratorContext::getFullyMatchedSubqueryGraph() const {
    auto subqueryGraph = SubqueryGraph(*mergedQueryGraph);
    for (auto i = 0u; i < mergedQueryGraph->getNumQueryNodes(); ++i) {
        subqueryGraph.addQueryNode(i);
    }
    for (auto i = 0u; i < mergedQueryGraph->getNumQueryRels(); ++i) {
        subqueryGraph.addQueryRel(i);
    }
    return subqueryGraph;
}

} // namespace planner
} // namespace graphflow
