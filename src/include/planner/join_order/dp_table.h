#pragma once

#include "binder/query/query_graph.h"
#include "join_tree.h"

namespace kuzu {
namespace planner {

class DPLevel {
public:
    bool contains(const binder::SubqueryGraph& subqueryGraph) const {
        return subgraphToJoinTree.contains(subqueryGraph);
    }
    const JoinTree& getJoinTree(const binder::SubqueryGraph& subqueryGraph) const {
        KU_ASSERT(contains(subqueryGraph));
        return subgraphToJoinTree.at(subqueryGraph);
    }

    void add(const binder::SubqueryGraph& subqueryGraph, const JoinTree& joinTree);

    const binder::subquery_graph_V_map_t<JoinTree>& getSubgraphAndJoinTrees() const {
        return subgraphToJoinTree;
    }

private:
    binder::subquery_graph_V_map_t<JoinTree> subgraphToJoinTree;
};

class DPTable {
public:
    void init(common::idx_t maxLevel);

    void add(const binder::SubqueryGraph& subqueryGraph, const JoinTree& joinTree);

    const DPLevel& getLevel(common::idx_t idx) const { return levels[idx]; }

private:
    std::vector<DPLevel> levels;
};

} // namespace planner
} // namespace kuzu
