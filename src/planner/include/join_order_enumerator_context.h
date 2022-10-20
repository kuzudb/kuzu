#pragma once

#include "src/binder/query/include/normalized_single_query.h"
#include "src/planner/include/subplans_table.h"
#include "src/planner/logical_plan/include/logical_plan.h"

namespace graphflow {
namespace planner {

class JoinOrderEnumeratorContext {
    friend class JoinOrderEnumerator;

public:
    JoinOrderEnumeratorContext()
        : currentLevel{0}, subPlansTable{make_unique<SubPlansTable>()},
          mergedQueryGraph{make_unique<QueryGraph>()} {}

    void init(const QueryGraph& queryGraph, const shared_ptr<Expression>& queryGraphPredicate,
        vector<unique_ptr<LogicalPlan>> prevPlans);

    inline expression_vector getWhereExpressions() { return whereExpressionsSplitOnAND; }

    inline bool containPlans(const SubqueryGraph& subqueryGraph) const {
        return subPlansTable->containSubgraphPlans(subqueryGraph);
    }
    inline vector<unique_ptr<LogicalPlan>>& getPlans(const SubqueryGraph& subqueryGraph) const {
        return subPlansTable->getSubgraphPlans(subqueryGraph);
    }
    inline void addPlan(const SubqueryGraph& subqueryGraph, unique_ptr<LogicalPlan> plan) {
        subPlansTable->addPlan(subqueryGraph, move(plan));
    }

    SubqueryGraph getEmptySubqueryGraph() const { return SubqueryGraph(*mergedQueryGraph); }
    /**
     * Returns a SubqueryGraph, which is a class used as a key in the subPlanTable, for
     * the MergedQueryGraph when all of its nodes and rels are matched
     */
    SubqueryGraph getFullyMatchedSubqueryGraph() const;
    inline void mergeQueryGraph(const QueryGraph& queryGraph) {
        mergedQueryGraph->merge(queryGraph);
    }
    inline QueryGraph* getQueryGraph() { return mergedQueryGraph.get(); }
    inline bool isNodeMatched(NodeExpression* node) {
        return matchedNodes.contains(node->getUniqueName());
    }
    inline void addMatchedNode(NodeExpression* node) {
        assert(!isNodeMatched(node));
        matchedNodes.insert(node->getUniqueName());
    }
    inline bool isRelMatched(RelExpression* rel) {
        return matchedRels.contains(rel->getUniqueName());
    }
    inline void addMatchedRel(RelExpression* rel) {
        assert(!isRelMatched(rel));
        matchedRels.insert(rel->getUniqueName());
    }
    inline bool nodeNeedScanTwice(NodeExpression* node) {
        for (auto& nodeToScanTwice : nodesToScanTwice) {
            if (nodeToScanTwice->getUniqueName() == node->getUniqueName()) {
                return true;
            }
        }
        return false;
    }

    void resetState();

private:
    expression_vector whereExpressionsSplitOnAND;

    uint32_t currentLevel;
    uint32_t maxLevel;

    unique_ptr<SubPlansTable> subPlansTable;
    unique_ptr<QueryGraph> mergedQueryGraph;
    // Avoid scan the same table twice.
    unordered_set<string> matchedNodes;
    unordered_set<string> matchedRels;

    LogicalPlan* outerPlan;
    expression_vector expressionsToScanFromOuter;
    vector<shared_ptr<NodeExpression>> nodesToScanTwice;
};

} // namespace planner
} // namespace graphflow
