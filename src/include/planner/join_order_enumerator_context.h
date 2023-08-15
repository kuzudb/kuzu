#pragma once

#include "binder/query/normalized_single_query.h"
#include "planner/logical_plan/logical_plan.h"
#include "planner/subplans_table.h"

namespace kuzu {
namespace planner {

enum class SubqueryType : uint8_t {
    NONE = 0,
    INTERNAL_ID_CORRELATED = 1,
    CORRELATED = 2,
};

class JoinOrderEnumeratorContext {
    friend class QueryPlanner;

public:
    JoinOrderEnumeratorContext()
        : currentLevel{0}, maxLevel{0}, subPlansTable{std::make_unique<SubPlansTable>()},
          queryGraph{nullptr}, subqueryType{SubqueryType::NONE}, correlatedExpressionsCardinality{
                                                                     1} {}

    void init(QueryGraph* queryGraph, const expression_vector& predicates);

    inline expression_vector getWhereExpressions() { return whereExpressionsSplitOnAND; }

    inline bool containPlans(const SubqueryGraph& subqueryGraph) const {
        return subPlansTable->containSubgraphPlans(subqueryGraph);
    }
    inline std::vector<std::unique_ptr<LogicalPlan>>& getPlans(
        const SubqueryGraph& subqueryGraph) const {
        return subPlansTable->getSubgraphPlans(subqueryGraph);
    }
    inline void addPlan(const SubqueryGraph& subqueryGraph, std::unique_ptr<LogicalPlan> plan) {
        subPlansTable->addPlan(subqueryGraph, std::move(plan));
    }

    inline SubqueryGraph getEmptySubqueryGraph() const { return SubqueryGraph(*queryGraph); }
    SubqueryGraph getFullyMatchedSubqueryGraph() const;

    inline QueryGraph* getQueryGraph() { return queryGraph; }

    inline binder::expression_vector getCorrelatedExpressions() const {
        return correlatedExpressions;
    }
    inline binder::expression_set getCorrelatedExpressionsSet() const {
        return binder::expression_set{correlatedExpressions.begin(), correlatedExpressions.end()};
    }
    void resetState();

private:
    expression_vector whereExpressionsSplitOnAND;

    uint32_t currentLevel;
    uint32_t maxLevel;

    std::unique_ptr<SubPlansTable> subPlansTable;
    QueryGraph* queryGraph;

    SubqueryType subqueryType;
    expression_vector correlatedExpressions;
    uint64_t correlatedExpressionsCardinality;
};

} // namespace planner
} // namespace kuzu
