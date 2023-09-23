#pragma once

#include "binder/query/query_graph.h"
#include "planner/operator/logical_plan.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/stats/rels_store_statistics.h"

namespace kuzu {
namespace planner {

class CardinalityEstimator {
public:
    CardinalityEstimator(const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
        const storage::RelsStoreStats& relsStatistics)
        : nodesStatistics{nodesStatistics}, relsStatistics{relsStatistics} {}

    void initNodeIDDom(binder::QueryGraph* queryGraph);

    uint64_t estimateScanNode(LogicalOperator* op);
    uint64_t estimateHashJoin(const binder::expression_vector& joinKeys,
        const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    uint64_t estimateCrossProduct(const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    uint64_t estimateIntersect(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& probePlan, const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);
    uint64_t estimateFlatten(const LogicalPlan& childPlan, f_group_pos groupPosToFlatten);
    uint64_t estimateFilter(const LogicalPlan& childPlan, const binder::Expression& predicate);

    double getExtensionRate(
        const binder::RelExpression& rel, const binder::NodeExpression& boundNode);

private:
    inline uint64_t atLeastOne(uint64_t x) { return x == 0 ? 1 : x; }

    inline void addNodeIDDom(const binder::NodeExpression& node) {
        auto key = node.getInternalID()->getUniqueName();
        if (!nodeIDName2dom.contains(key)) {
            nodeIDName2dom.insert({key, getNumNodes(node)});
        }
    }
    inline uint64_t getNodeIDDom(const std::string& nodeIDName) {
        assert(nodeIDName2dom.contains(nodeIDName));
        return nodeIDName2dom.at(nodeIDName);
    }
    uint64_t getNumNodes(const binder::NodeExpression& node);

    uint64_t getNumRels(const binder::RelExpression& rel);

private:
    const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics;
    const storage::RelsStoreStats& relsStatistics;
    // The domain of nodeID is defined as the number of unique value of nodeID, i.e. num nodes.
    std::unordered_map<std::string, uint64_t> nodeIDName2dom;
};

} // namespace planner
} // namespace kuzu
