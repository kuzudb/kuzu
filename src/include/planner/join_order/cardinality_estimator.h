#pragma once

#include "binder/query/query_graph.h"
#include "planner/operator/logical_plan.h"

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
    void addNodeIDDom(const transaction::Transaction* transaction, const binder::Expression& nodeID,
        const std::vector<common::table_id_t>& tableIDs);

    uint64_t estimateScanNode(LogicalOperator* op);
    uint64_t estimateHashJoin(const binder::expression_vector& joinKeys,
        const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    uint64_t estimateCrossProduct(const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    uint64_t estimateIntersect(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& probePlan, const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);
    uint64_t estimateFlatten(const LogicalPlan& childPlan, f_group_pos groupPosToFlatten);
    uint64_t estimateFilter(const LogicalPlan& childPlan, const binder::Expression& predicate);

    double getExtensionRate(const binder::RelExpression& rel,
        const binder::NodeExpression& boundNode, const transaction::Transaction* transaction);

private:
    uint64_t atLeastOne(uint64_t x) { return x == 0 ? 1 : x; }

    uint64_t getNodeIDDom(const std::string& nodeIDName) {
        KU_ASSERT(nodeIDName2dom.contains(nodeIDName));
        return nodeIDName2dom.at(nodeIDName);
    }
    uint64_t getNumNodes(const transaction::Transaction* transaction,
        const std::vector<common::table_id_t>& tableIDs);
    uint64_t getNumRels(const transaction::Transaction* transaction,
        const std::vector<common::table_id_t>& tableIDs);

private:
    main::ClientContext* context;
    // The domain of nodeID is defined as the number of unique value of nodeID, i.e. num nodes.
    std::unordered_map<std::string, uint64_t> nodeIDName2dom;
};

} // namespace planner
} // namespace kuzu
