#pragma once

#include "src/binder/query/include/normalized_single_query.h"
#include "src/planner/include/subplans_table.h"
#include "src/planner/logical_plan/include/logical_plan.h"

namespace kuzu {
namespace planner {

class JoinOrderEnumeratorContext {
    friend class JoinOrderEnumerator;

public:
    JoinOrderEnumeratorContext()
        : currentLevel{0}, maxLevel{0}, subPlansTable{make_unique<SubPlansTable>()}, outerPlan{
                                                                                         nullptr} {}

    void init(QueryGraph* queryGraph, expression_vector& predicates);

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

    inline SubqueryGraph getEmptySubqueryGraph() const { return SubqueryGraph(*queryGraph); }
    SubqueryGraph getFullyMatchedSubqueryGraph() const;

    inline QueryGraph* getQueryGraph() { return queryGraph; }

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
    QueryGraph* queryGraph;

    LogicalPlan* outerPlan;
    expression_vector expressionsToScanFromOuter;
    vector<shared_ptr<NodeExpression>> nodesToScanTwice;
};

} // namespace planner
} // namespace kuzu
