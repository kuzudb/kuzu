#pragma once

#include "binder/query/reading_clause/query_graph.h"
#include "planner/logical_plan/logical_plan.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace planner {

class CardinalityEstimator {
public:
    CardinalityEstimator(const storage::NodesStatisticsAndDeletedIDs& nodesStatistics,
        const storage::RelsStatistics& relsStatistics)
        : nodesStatistics{nodesStatistics}, relsStatistics{relsStatistics} {}

    void initNodeIDDom(binder::QueryGraph* queryGraph);

    uint64_t estimateScanNode(LogicalOperator* op);
    uint64_t estimateHashJoin(const binder::expression_vector& joinNodeIDs,
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
        if (!nodeIDName2dom.contains(node.getInternalIDPropertyName())) {
            nodeIDName2dom.insert({node.getInternalIDPropertyName(), getNumNodes(node)});
        }
    }
    uint64_t getNodeIDDom(const std::string& nodeIDName) {
        assert(nodeIDName2dom.contains(nodeIDName));
        return nodeIDName2dom.at(nodeIDName);
    }
    uint64_t getNumNodes(const binder::NodeExpression& node);

    uint64_t getNumRels(const binder::RelExpression& rel);

private:
    const storage::NodesStatisticsAndDeletedIDs& nodesStatistics;
    const storage::RelsStatistics& relsStatistics;
    // The domain of nodeID is defined as the number of unique value of nodeID, i.e. num nodes.
    std::unordered_map<std::string, uint64_t> nodeIDName2dom;
};

} // namespace planner
} // namespace kuzu
