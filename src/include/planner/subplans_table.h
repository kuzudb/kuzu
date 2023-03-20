#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "binder/query/reading_clause/query_graph.h"
#include "planner/logical_plan/logical_plan.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

const uint64_t MAX_LEVEL_TO_PLAN_EXACTLY = 7;
const uint64_t MAX_NUM_SUBGRAPHS_PER_LEVEL = 50;
const uint64_t MAX_NUM_PLANS_PER_SUBGRAPH = 50;

class SubPlansTable {
    struct PlanSet;
    // Each dp level is a map from sub query graph to a set of plans
    using dp_level_t = subquery_graph_V_map_t<std::unique_ptr<PlanSet>>;

public:
    void resize(uint32_t newSize);

    bool containSubgraphPlans(const SubqueryGraph& subqueryGraph) const;

    std::vector<std::unique_ptr<LogicalPlan>>& getSubgraphPlans(const SubqueryGraph& subqueryGraph);

    std::vector<SubqueryGraph> getSubqueryGraphs(uint32_t level);

    void addPlan(const SubqueryGraph& subqueryGraph, std::unique_ptr<LogicalPlan> plan);

    void clear();

private:
    struct PlanSet {
        std::vector<std::unique_ptr<LogicalPlan>> plans;
        schema_map_t<common::vector_idx_t> schemaToPlanIdx;

        inline std::vector<std::unique_ptr<LogicalPlan>>& getPlans() { return plans; }

        void addPlan(std::unique_ptr<LogicalPlan> plan);
    };

    dp_level_t* getDPLevel(const SubqueryGraph& subqueryGraph) const {
        return dpLevels[subqueryGraph.getTotalNumVariables()].get();
    }

private:
    std::vector<std::unique_ptr<dp_level_t>> dpLevels;
};

} // namespace planner
} // namespace kuzu
