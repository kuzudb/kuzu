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
    inline const bitset<MAX_NUM_VARIABLES>& getMatchedQueryRels() const { return matchedQueryRels; }
    inline const bitset<MAX_NUM_VARIABLES>& getMatchedQueryNodes() const {
        return matchedQueryNodes;
    }

    inline void setExpressionsToScanFromOuter(expression_vector expressions) {
        expressionsToScanFromOuter = move(expressions);
    }
    inline void clearExpressionsToScanFromOuter() { expressionsToScanFromOuter.clear(); }
    inline bool hasExpressionsToScanFromOuter() { return !expressionsToScanFromOuter.empty(); }
    inline const expression_vector& getExpressionsToScanFromOuter() {
        return expressionsToScanFromOuter;
    }

    void resetState();

private:
    expression_vector whereExpressionsSplitOnAND;

    uint32_t currentLevel;
    uint32_t maxLevel;

    unique_ptr<SubPlansTable> subPlansTable;
    unique_ptr<QueryGraph> mergedQueryGraph;
    // We keep track of query nodes and rels matched in previous query graph so that new query part
    // enumeration does not enumerate a rel that exist in previous query parts
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
    bitset<MAX_NUM_VARIABLES> matchedQueryNodes;

    expression_vector expressionsToScanFromOuter;
};

} // namespace planner
} // namespace graphflow
