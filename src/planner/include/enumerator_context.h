#pragma once

#include "src/binder/include/query_graph/query_graph.h"
#include "src/planner/include/subplans_table.h"

namespace graphflow {
namespace planner {

/**
 * This context contains local states (states that are not shared between)
 */
class EnumeratorContext {

public:
    EnumeratorContext()
        : currentLevel{0}, subPlansTable{make_unique<SubPlansTable>()},
          mergedQueryGraph{make_unique<QueryGraph>()}, matchedQueryRels{}, matchedQueryNodes{},
          outerQuerySchema{nullptr} {}

    inline bool hasNextLevel() const { return currentLevel < mergedQueryGraph->getNumQueryRels(); }
    inline uint32_t getCurrentLevel() const { return currentLevel; }
    inline void incrementCurrentLevel() { currentLevel++; }

    void initSubPlansTable(uint32_t maxSize) { subPlansTable->init(maxSize); }
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

    void prepareQueryGraph(QueryGraph& queryGraph);
    SubqueryGraph getEmptySubqueryGraph() const;
    SubqueryGraph getFullyMatchedSubqueryGraph() const;

    inline QueryGraph* getQueryGraph() { return mergedQueryGraph.get(); }
    inline const bitset<MAX_NUM_VARIABLES>& getMatchedQueryRels() const { return matchedQueryRels; }
    inline const bitset<MAX_NUM_VARIABLES>& getMatchedQueryNodes() const {
        return matchedQueryNodes;
    }

    inline bool isSubqueryEnumeration() const { return outerQuerySchema != nullptr; }
    inline Schema* getOuterQuerySchema() const { return outerQuerySchema.get(); }
    inline void setOuterQuerySchema(unique_ptr<Schema> schema) {
        outerQuerySchema = move(schema);
    }

public:
    uint32_t currentLevel;
    unique_ptr<SubPlansTable> subPlansTable;

    unique_ptr<QueryGraph> mergedQueryGraph;
    // query nodes and rels matched in previous query graph
    bitset<MAX_NUM_VARIABLES> matchedQueryRels;
    bitset<MAX_NUM_VARIABLES> matchedQueryNodes;

    unique_ptr<Schema> outerQuerySchema;
    vector<shared_ptr<NodeExpression>> outerQueryNodesToSelect;
};

} // namespace planner
} // namespace graphflow