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
    CardinalityEstimator() = default;
    explicit CardinalityEstimator(main::ClientContext* context) : context{context} {}
    DELETE_COPY_DEFAULT_MOVE(CardinalityEstimator);

    // TODO(Xiyang): revisit this init at some point. Maybe we should init while enumerating.
    void initNodeIDDom(const binder::QueryGraph& queryGraph, transaction::Transaction* transaction);
    void addNodeIDDom(const binder::NodeExpression& node);
    void addNodeIDDom(const binder::Expression& nodeID,
        const std::vector<catalog::TableCatalogEntry*>& entries);

    uint64_t estimateScanNode(LogicalOperator* op);
    uint64_t estimateHashJoin(const binder::expression_vector& joinKeys,
        const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    uint64_t estimateCrossProduct(const LogicalPlan& probePlan, const LogicalPlan& buildPlan);
    uint64_t estimateIntersect(const binder::expression_vector& joinNodeIDs,
        const LogicalPlan& probePlan, const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);
    uint64_t estimateFlatten(const LogicalPlan& childPlan, f_group_pos groupPosToFlatten);
    uint64_t estimateFilter(const LogicalPlan& childPlan, const binder::Expression& predicate);

    double getExtensionRate(const binder::RelExpression& rel,
        const binder::NodeExpression& boundNode);

private:
    common::offset_t getNodeIDDom(const std::string& nodeIDName) {
        KU_ASSERT(nodeIDName2dom.contains(nodeIDName));
        return nodeIDName2dom.at(nodeIDName);
    }
    common::offset_t getNumNodes(const std::vector<catalog::TableCatalogEntry*>& entries);

    common::offset_t getNumRels(const std::vector<catalog::TableCatalogEntry*>& tableIDs);

private:
    main::ClientContext* context;
    // The domain of nodeID is defined as the number of unique value of nodeID, i.e. num nodes.
    std::unordered_map<std::string, common::offset_t> nodeIDName2dom;
};

} // namespace planner
} // namespace kuzu
