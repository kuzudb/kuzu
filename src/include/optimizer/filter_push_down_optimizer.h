#pragma once

#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace optimizer {

struct PredicateSet {
    binder::expression_vector equalityPredicates;
    binder::expression_vector nonEqualityPredicates;

    PredicateSet() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(PredicateSet);

    bool isEmpty() const { return equalityPredicates.empty() && nonEqualityPredicates.empty(); }
    void clear() {
        equalityPredicates.clear();
        nonEqualityPredicates.clear();
    }

    void addPredicate(std::shared_ptr<binder::Expression> predicate);
    std::shared_ptr<binder::Expression> popNodePKEqualityComparison(
        const binder::Expression& nodeID);
    binder::expression_vector getAllPredicates();

private:
    PredicateSet(const PredicateSet& other)
        : equalityPredicates{other.equalityPredicates},
          nonEqualityPredicates{other.nonEqualityPredicates} {}
};

class FilterPushDownOptimizer {
public:
    FilterPushDownOptimizer() { predicateSet = PredicateSet(); }
    explicit FilterPushDownOptimizer(PredicateSet predicateSet)
        : predicateSet{std::move(predicateSet)} {}

    void rewrite(planner::LogicalPlan* plan);

private:
    std::shared_ptr<planner::LogicalOperator> visitOperator(
        const std::shared_ptr<planner::LogicalOperator>& op);
    // Collect predicates in FILTER
    std::shared_ptr<planner::LogicalOperator> visitFilterReplace(
        const std::shared_ptr<planner::LogicalOperator>& op);
    // Push primary key lookup into CROSS_PRODUCT
    // E.g.
    //      Filter(a.ID=b.ID)
    //      CrossProduct                   to           IndexNestedLoopJoin(b)
    //   S(a)           S(b)                            S(a)
    // This is a temporary solution in the absence of a generic hash join operator.
    std::shared_ptr<planner::LogicalOperator> visitCrossProductReplace(
        const std::shared_ptr<planner::LogicalOperator>& op);

    // TODO(Xiyang/Guodong): This should be reworked for pushing filters into ScanNodeTable.
    //                       Also, should be reworked for IndexScan.
    // Push FILTER into SCAN_NODE_TABLE, and turn index lookup into INDEX_SCAN.
    std::shared_ptr<planner::LogicalOperator> visitScanNodePropertyReplace(
        const std::shared_ptr<planner::LogicalOperator>& op);

    // Push down FILTER into SCAN_NODE_TABLE.
    std::shared_ptr<planner::LogicalOperator> pushDownToScanNode(
        std::shared_ptr<binder::Expression> nodeID, std::vector<common::table_id_t> tableIDs,
        std::shared_ptr<binder::Expression> predicate,
        std::shared_ptr<planner::LogicalOperator> child);

    // Finish the current push down optimization by apply remaining predicates as a single filter.
    // And heuristically reorder equality predicates first in the filter.
    std::shared_ptr<planner::LogicalOperator> finishPushDown(
        std::shared_ptr<planner::LogicalOperator> op);
    std::shared_ptr<planner::LogicalOperator> appendFilters(
        const binder::expression_vector& predicates,
        std::shared_ptr<planner::LogicalOperator> child);

    std::shared_ptr<planner::LogicalOperator> appendScanNodeTable(
        std::shared_ptr<binder::Expression> nodeID, std::vector<common::table_id_t> nodeTableIDs,
        binder::expression_vector properties, std::shared_ptr<planner::LogicalOperator> child);
    std::shared_ptr<planner::LogicalOperator> appendFilter(
        std::shared_ptr<binder::Expression> predicate,
        std::shared_ptr<planner::LogicalOperator> child);

private:
    PredicateSet predicateSet;
};

} // namespace optimizer
} // namespace kuzu
