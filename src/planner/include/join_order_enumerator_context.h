#pragma once

#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/norm_query/normalized_query.h"
#include "src/planner/include/subplans_table.h"

namespace graphflow {
namespace planner {

class JoinOrderEnumeratorContext {

public:
    JoinOrderEnumeratorContext()
        : currentLevel{0}, subPlansTable{make_unique<SubPlansTable>()},
          mergedQueryGraph{make_unique<QueryGraph>()} {}

    void init(const QueryGraph& queryGraph, const shared_ptr<Expression>& queryGraphPredicate,
        vector<unique_ptr<LogicalPlan>> prevPlans);

    inline vector<shared_ptr<Expression>> getWhereExpressions() {
        return whereExpressionsSplitOnAND;
    }

    inline bool hasNextLevel() const { return currentLevel < mergedQueryGraph->getNumQueryRels(); }
    inline uint32_t getCurrentLevel() const { return currentLevel; }
    inline void incrementCurrentLevel() { currentLevel++; }

    inline SubqueryGraphPlansMap& getSubqueryGraphPlansMap(uint32_t level) const {
        return subPlansTable->getSubqueryGraphPlansMap(level);
    }
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

    inline void setExpressionsToScanFromOuter(vector<shared_ptr<Expression>> expressions) {
        expressionsToScanFromOuter = move(expressions);
    }
    inline void clearExpressionsToScanFromOuter() { expressionsToScanFromOuter.clear(); }
    inline bool hasExpressionsToScanFromOuter() { return !expressionsToScanFromOuter.empty(); }
    inline const vector<shared_ptr<Expression>>& getExpressionsToScanFromOuter() {
        return expressionsToScanFromOuter;
    }

private:
    vector<shared_ptr<Expression>> whereExpressionsSplitOnAND;

    uint32_t currentLevel;
    unique_ptr<SubPlansTable> subPlansTable;
    unique_ptr<QueryGraph> mergedQueryGraph;
    // We keep track of query nodes and rels matched in previous query graph so that new query part
    // enumeration does not enumerate a rel that exist in previous query parts
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
    bitset<MAX_NUM_VARIABLES> matchedQueryNodes;

    vector<shared_ptr<Expression>> expressionsToScanFromOuter;
};

} // namespace planner
} // namespace graphflow
