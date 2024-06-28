#pragma once

#include "binder/query/query_graph.h"
#include "cardinality_estimator.h"
#include "dp_table.h"
#include "join_tree.h"
#include "planner/join_order_enumerator_context.h"

namespace kuzu {
namespace planner {

class PropertyExprCollection {
public:
    binder::expression_vector getProperties(std::shared_ptr<binder::Expression> pattern) const {
        if (!patternToProperties.contains(pattern)) {
            return binder::expression_vector{};
        }
        return patternToProperties.at(pattern);
    }

    void addProperties(std::shared_ptr<binder::Expression> pattern,
        const binder::expression_vector& properties) {
        KU_ASSERT(!patternToProperties.contains(pattern));
        patternToProperties.insert({pattern, properties});
    }

private:
    binder::expression_map<binder::expression_vector> patternToProperties;
};

/*
 * JoinOrderSolver solves a reasonable join order for
 */
class JoinOrderSolver {
public:
    explicit JoinOrderSolver(const binder::QueryGraph& queryGraph,
        binder::expression_vector predicates, PropertyExprCollection propertyExprCollection,
        main::ClientContext* context)
        : queryGraph{queryGraph}, queryGraphPredicates{std::move(predicates)},
          propertyCollection{std::move(propertyExprCollection)}, context{context} {}

    void setCorrExprs(SubqueryType subqueryType_, binder::expression_vector exprs,
        cardianlity_t card) {
        subqueryType = subqueryType_;
        corrExprs = std::move(exprs);
        corrExprsCardinality = card;
    }

    JoinTree solve();

private:
    void planLevel(common::idx_t level);
    void planBaseScans();
    void planCorrelatedExpressionsScan(const binder::SubqueryGraph& newSubgraph);
    void planBaseNodeScan(common::idx_t nodeIdx);
    void planBaseRelScan(common::idx_t relIdx);
    void planBinaryJoin(common::idx_t leftSize, common::idx_t rightSize);
    void planWorstCaseOptimalJoin(common::idx_t size, common::idx_t otherSize);
    void planBinaryJoin(const binder::SubqueryGraph& subqueryGraph, const JoinTree& joinTree,
        const binder::SubqueryGraph& otherSubqueryGraph, const JoinTree& otherJoinTree,
        std::vector<std::shared_ptr<binder::NodeExpression>> joinNodes);
    void planHashJoin(const JoinTree& joinTree, const JoinTree& otherJoinTree,
        std::vector<std::shared_ptr<binder::NodeExpression>> joinNodes,
        const binder::SubqueryGraph& newSubqueryGraph, const binder::expression_vector& predicates);
    void planWorstCaseOptimalJoin(const JoinTree& joinTree,
        const std::vector<JoinTree>& relJoinTrees, std::shared_ptr<binder::NodeExpression> joinNode,
        const binder::SubqueryGraph& newSubqueryGraph, const binder::expression_vector& predicates);
    bool tryPlanIndexNestedLoopJoin(const JoinTree& joinTree, const JoinTree& otherJoinTree,
        std::shared_ptr<binder::NodeExpression> joinNode,
        const binder::SubqueryGraph& newSubqueryGraph, const binder::expression_vector& predicates);

private:
    // Query graph to plan
    const binder::QueryGraph& queryGraph;
    // Predicates to apply for given query graph
    binder::expression_vector queryGraphPredicates;
    //
    SubqueryPlanInfo subqueryPlanInfo;
    // Properties to scan for given query graph.
    PropertyExprCollection propertyCollection;

    main::ClientContext* context;
    DPTable dpTable;
    CardinalityEstimator cardinalityEstimator;
};

} // namespace planner
} // namespace kuzu
