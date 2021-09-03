#include "src/planner/include/join_order_enumerator_context.h"

namespace graphflow {
namespace planner {

void JoinOrderEnumeratorContext::init(
    const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans) {
    // split where expression
    whereExpressionsSplitOnAND = queryPart.hasWhereExpression() ?
                                     queryPart.getWhereExpression()->splitOnAND() :
                                     vector<shared_ptr<Expression>>();
    // merge new query graph
    auto fullyMatchedSubqueryGraph = getFullyMatchedSubqueryGraph();
    matchedQueryRels = fullyMatchedSubqueryGraph.queryRelsSelector;
    matchedQueryNodes = fullyMatchedSubqueryGraph.queryNodesSelector;
    mergedQueryGraph->merge(*queryPart.getQueryGraph());
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

void JoinOrderEnumeratorContext::populatePropertiesMap(const NormalizedQueryPart& queryPart) {
    variableToPropertiesMap.clear();
    for (auto& propertyExpression : queryPart.getDependentPropertiesFromWhereAndProjection()) {
        auto variableName = propertyExpression->children[0]->getInternalName();
        if (!variableToPropertiesMap.contains(variableName)) {
            variableToPropertiesMap.insert({variableName, vector<shared_ptr<Expression>>()});
        }
        variableToPropertiesMap.at(variableName).push_back(propertyExpression);
    }
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
