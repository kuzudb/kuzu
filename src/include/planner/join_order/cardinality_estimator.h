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

    // TODO(Xiyang): revisit this init at some point. Maybe we should init while enumerating.
    void initNodeIDDom(binder::QueryGraph* queryGraph);
    void addNodeIDDom(
        const binder::Expression& nodeID, const std::vector<common::table_id_t>& tableIDs);

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

    inline uint64_t getNodeIDDom(const std::string& nodeIDName) {
        KU_ASSERT(nodeIDName2dom.contains(nodeIDName));
        return nodeIDName2dom.at(nodeIDName);
    }
    uint64_t getNumNodes(const std::vector<common::table_id_t>& tableIDs);

    uint64_t getNumRels(const std::vector<common::table_id_t>& tableIDs);

private:
    const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics;
    const storage::RelsStoreStats& relsStatistics;
    // The domain of nodeID is defined as the number of unique value of nodeID, i.e. num nodes.
    std::unordered_map<std::string, uint64_t> nodeIDName2dom;
};

} // namespace planner
} // namespace kuzu
