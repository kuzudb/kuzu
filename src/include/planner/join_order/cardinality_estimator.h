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
    void addNodeIDDomOverride(const binder::Expression& nodeID, cardinality_t numNodes);

    cardinality_t estimateScanNode(const LogicalOperator& op) const;
    cardinality_t estimateExtend(double extensionRate, const LogicalOperator& childOp) const;
    cardinality_t estimateHashJoin(const binder::expression_vector& joinKeys,
        const LogicalOperator& probeOp, const LogicalOperator& buildOp) const;
    cardinality_t estimateCrossProduct(const LogicalOperator& probeOp,
        const LogicalOperator& buildOp) const;
    cardinality_t estimateIntersect(const binder::expression_vector& joinNodeIDs,
        const LogicalOperator& probeOp, const std::vector<LogicalOperator*>& buildOps) const;
    cardinality_t estimateFlatten(const LogicalOperator& childOp,
        f_group_pos groupPosToFlatten) const;
    cardinality_t estimateFilter(const LogicalOperator& childOp,
        const binder::Expression& predicate) const;

    double getExtensionRate(const binder::RelExpression& rel,
        const binder::NodeExpression& boundNode, const transaction::Transaction* transaction) const;

private:
    cardinality_t getNodeIDDom(const std::string& nodeIDName) const;
    cardinality_t getNumNodes(const transaction::Transaction* transaction,
        const std::vector<common::table_id_t>& tableIDs) const;
    cardinality_t getNumRels(const transaction::Transaction* transaction,
        const std::vector<common::table_id_t>& tableIDs) const;

private:
    main::ClientContext* context;
    // TODO(Guodong): Extend this to cover rel tables.
    std::unordered_map<common::table_id_t, storage::TableStats> nodeTableStats;
    // The domain of nodeID is defined as the number of unique value of nodeID, i.e. num nodes.
    std::unordered_map<std::string, cardinality_t> nodeIDName2dom;
    // per-query-graph overrides for nodeIDName2dom
    std::unordered_map<std::string, cardinality_t> nodeIDName2domOverride;
};

} // namespace planner
} // namespace kuzu
