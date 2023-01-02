#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "binder/query/reading_clause/query_graph.h"
#include "planner/logical_plan/logical_plan.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class SubPlansTable {
    typedef unordered_map<SubqueryGraph, vector<unique_ptr<LogicalPlan>>, SubqueryGraphHasher>
        SubqueryGraphPlansMap;

public:
    void resize(uint32_t newSize);

    bool containSubgraphPlans(const SubqueryGraph& subqueryGraph) const;

    vector<unique_ptr<LogicalPlan>>& getSubgraphPlans(const SubqueryGraph& subqueryGraph);

    vector<SubqueryGraph> getSubqueryGraphs(uint32_t level);

    void addPlan(const SubqueryGraph& subqueryGraph, unique_ptr<LogicalPlan> plan);
    void finalizeLevel(uint32_t level);

    void clear();

private:
    vector<unique_ptr<SubqueryGraphPlansMap>> subPlans;
};

} // namespace planner
} // namespace kuzu
