#pragma once

#include "binder/query/query_graph.h"
#include "planner/operator/logical_plan.h"
#include "storage/stats/table_stats.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main

namespace transaction {
class Transaction;
} // namespace transaction

namespace planner {

class CardinalityEstimator {
public:
    CardinalityEstimator() : context{nullptr} {};
    explicit CardinalityEstimator(main::ClientContext* context) : context{context} {}
    DELETE_COPY_DEFAULT_MOVE(CardinalityEstimator);

    // TODO(Xiyang): revisit this init at some point. Maybe we should init while enumerating.
    void initNodeIDDom(const transaction::Transaction* transaction,
        const binder::QueryGraph& queryGraph);
    void addNodeIDDomAndStats(const transaction::Transaction* transaction,
        const binder::Expression& nodeID, const std::vector<common::table_id_t>& tableIDs);

    cardinality_t estimateScanNode(LogicalOperator* op);
    cardinality_t estimateExtend(double extensionRate, const LogicalPlan& childPlan);
    cardinality_t estimateHashJoin(const binder::expression_vector& joinKeys,
        const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    cardinality_t estimateCrossProduct(const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    cardinality_t estimateIntersect(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& probePlan, const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);
    cardinality_t estimateFlatten(const LogicalPlan& childPlan, f_group_pos groupPosToFlatten);
    cardinality_t estimateFilter(const LogicalPlan& childPlan, const binder::Expression& predicate);

    double getExtensionRate(const binder::RelExpression& rel,
        const binder::NodeExpression& boundNode, const transaction::Transaction* transaction);

private:
    cardinality_t atLeastOne(uint64_t x) { return x == 0 ? 1 : x; }

    cardinality_t getNodeIDDom(const std::string& nodeIDName) {
        KU_ASSERT(nodeIDName2dom.contains(nodeIDName));
        return nodeIDName2dom.at(nodeIDName);
    }
    cardinality_t getNumNodes(const transaction::Transaction* transaction,
        const std::vector<common::table_id_t>& tableIDs);
    cardinality_t getNumRels(const transaction::Transaction* transaction,
        const std::vector<common::table_id_t>& tableIDs);

private:
    main::ClientContext* context;
    // TODO(Guodong): Extend this to cover rel tables.
    std::unordered_map<common::table_id_t, storage::TableStats> nodeTableStats;
    // The domain of nodeID is defined as the number of unique value of nodeID, i.e. num nodes.
    std::unordered_map<std::string, cardinality_t> nodeIDName2dom;
};

} // namespace planner
} // namespace kuzu
