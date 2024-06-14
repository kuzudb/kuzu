#include "planner/join_order/dp_table.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void DPLevel::add(const SubqueryGraph& subqueryGraph, const JoinTree& joinTree) {
    if (!contains(subqueryGraph)) {
        subgraphToJoinTree.insert({subqueryGraph, joinTree});
        return;
    }
    auto& currentTree = subgraphToJoinTree.at(subqueryGraph);
    if (currentTree.cost > joinTree.cost) {
        subgraphToJoinTree.erase(subqueryGraph);
        subgraphToJoinTree.insert({subqueryGraph, joinTree});
    }
}

void DPTable::init(common::idx_t maxLevel) {
    levels.resize(maxLevel + 1);
}

void DPTable::add(const binder::SubqueryGraph& subqueryGraph, const JoinTree& joinTree) {
    auto l = subqueryGraph.getNumQueryNodes() + subqueryGraph.getNumQueryRels();
    auto& level = levels[subqueryGraph.getNumQueryNodes() + subqueryGraph.getNumQueryRels()];
    level.add(subqueryGraph, joinTree);
}

}
}