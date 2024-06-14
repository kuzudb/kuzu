#pragma once

#include "binder/query/query_graph.h"
#include "planner/operator/logical_plan.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/stats/rels_store_statistics.h"

namespace kuzu {
namespace planner {

class CardinalityEstimator {
public:
    CardinalityEstimator() = default;
    CardinalityEstimator(main::ClientContext* context,
        const storage::NodesStoreStatsAndDeletedIDs* nodesStatistics,
        const storage::RelsStoreStats* relsStatistics)
        : context{context}, nodesStatistics{nodesStatistics}, relsStatistics{relsStatistics} {}
    DELETE_COPY_DEFAULT_MOVE(CardinalityEstimator);

    // TODO(Xiyang): revisit this init at some point. Maybe we should init while enumerating.
    void initNodeIDDom(const binder::QueryGraph& queryGraph, transaction::Transaction* transaction);
    void addNodeIDDom(const binder::Expression& nodeID,
        const std::vector<common::table_id_t>& tableIDs, transaction::Transaction* transaction);

    cardianlity_t estimateScanNode(LogicalOperator* op);
    cardianlity_t estimateHashJoin(const binder::expression_vector& joinKeys,
        const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    cardianlity_t estimateHashJoin(const binder::expression_vector& joinKeys,
        cardianlity_t probeCard, cardianlity_t buildCard);
    cardianlity_t estimateCrossProduct(const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    cardianlity_t estimateIntersect(const binder::expression_vector& joinNodeIDs,
        cardianlity_t probeCard, const std::vector<cardianlity_t>& buildCard);
    cardianlity_t estimateFlatten(const LogicalPlan& childPlan, f_group_pos groupPosToFlatten);
    cardianlity_t estimateFilters(cardianlity_t inCardinality, const binder::expression_vector& predicates);
    cardianlity_t estimateFilter(cardianlity_t inCardinality, const binder::Expression& predicate);

    double getExtensionRate(const binder::RelExpression& rel,
        const binder::NodeExpression& boundNode, transaction::Transaction* transaction);
    cardianlity_t getNumNodes(const std::vector<common::table_id_t>& tableIDs,
        transaction::Transaction* transaction);
    cardianlity_t getNumRels(const std::vector<common::table_id_t>& tableIDs,
        transaction::Transaction* transaction);

private:
    uint64_t atLeastOne(uint64_t x) { return x == 0 ? 1 : x; }

    uint64_t getNodeIDDom(const std::string& nodeIDName) {
        KU_ASSERT(nodeIDName2dom.contains(nodeIDName));
        return nodeIDName2dom.at(nodeIDName);
    }

private:
    main::ClientContext* context;
    const storage::NodesStoreStatsAndDeletedIDs* nodesStatistics;
    const storage::RelsStoreStats* relsStatistics;
    // The domain of nodeID is defined as the number of unique value of nodeID, i.e. num nodes.
    std::unordered_map<std::string, uint64_t> nodeIDName2dom;
};

} // namespace planner
} // namespace kuzu
