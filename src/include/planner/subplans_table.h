#pragma once

#include <unordered_map>

#include "binder/query/query_graph.h"
#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace planner {

const uint64_t MAX_LEVEL_TO_PLAN_EXACTLY = 7;

// Different from vanilla dp algorithm where one optimal plan is kept per subgraph, we keep multiple
// plans each with a different factorization structure. The following example will explain our
// rationale.
// Given a triangle with an outgoing edge
// MATCH (a)->(b)->(c), (a)->(c), (c)->(d)
// At level 3 (assume level is based on num of nodes) for subgraph "abc", if we ignore factorization
// structure, the 3 plans that intersects on "a", "b", or "c" are considered homogenous and one of
// them will be picked.
// Then at level 4 for subgraph "abcd", we know the plan that intersect on "c" will be worse because
// we need to further flatten it and extend to "d".
// Therefore, we try to be factorization aware when keeping optimal plans.
class SubgraphPlans {
public:
    explicit SubgraphPlans(const binder::SubqueryGraph& subqueryGraph);

    inline uint64_t getMaxCost() const { return maxCost; }

    void addPlan(std::unique_ptr<LogicalPlan> plan);

    std::vector<std::unique_ptr<LogicalPlan>>& getPlans() { return plans; }

private:
    // To balance computation time, we encode plan by only considering the flat information of the
    // nodes that are involved in current subgraph.
    std::bitset<binder::MAX_NUM_QUERY_VARIABLES> encodePlan(const LogicalPlan& plan);

private:
    constexpr static uint32_t MAX_NUM_PLANS = 10;

private:
    uint64_t maxCost = UINT64_MAX;
    binder::expression_vector nodeIDsToEncode;
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    std::unordered_map<std::bitset<binder::MAX_NUM_QUERY_VARIABLES>, common::idx_t>
        encodedPlan2PlanIdx;
};

// A DPLevel is a collection of plans per subgraph. All subgraph should have the same number of
// variables.
class DPLevel {
public:
    inline bool contains(const binder::SubqueryGraph& subqueryGraph) {
        return subgraph2Plans.contains(subqueryGraph);
    }

    inline SubgraphPlans* getSubgraphPlans(const binder::SubqueryGraph& subqueryGraph) {
        return subgraph2Plans.at(subqueryGraph).get();
    }

    std::vector<binder::SubqueryGraph> getSubqueryGraphs();

    void addPlan(const binder::SubqueryGraph& subqueryGraph, std::unique_ptr<LogicalPlan> plan);

    inline void clear() { subgraph2Plans.clear(); }

private:
    constexpr static uint32_t MAX_NUM_SUBGRAPH = 50;

private:
    binder::subquery_graph_V_map_t<std::unique_ptr<SubgraphPlans>> subgraph2Plans;
};

class SubPlansTable {
public:
    void resize(uint32_t newSize);

    uint64_t getMaxCost(const binder::SubqueryGraph& subqueryGraph) const;

    bool containSubgraphPlans(const binder::SubqueryGraph& subqueryGraph) const;

    std::vector<std::unique_ptr<LogicalPlan>>& getSubgraphPlans(
        const binder::SubqueryGraph& subqueryGraph);

    std::vector<binder::SubqueryGraph> getSubqueryGraphs(uint32_t level);

    void addPlan(const binder::SubqueryGraph& subqueryGraph, std::unique_ptr<LogicalPlan> plan);

    void clear();

private:
    DPLevel* getDPLevel(const binder::SubqueryGraph& subqueryGraph) const {
        return dpLevels[subqueryGraph.getTotalNumVariables()].get();
    }

private:
    std::vector<std::unique_ptr<DPLevel>> dpLevels;
};

} // namespace planner
} // namespace kuzu
