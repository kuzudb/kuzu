#pragma once

#include "binder/query/normalized_single_query.h"
#include "catalog/catalog.h"
#include "common/join_type.h"
#include "planner/join_order_enumerator_context.h"
#include "planner/logical_plan/logical_operator/extend_direction.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace planner {

class QueryPlanner;
class JoinOrderEnumeratorContext;

/**
 * JoinOrderEnumerator is currently responsible for
 *      join order enumeration
 *      filter push down
 */
class JoinOrderEnumerator {
public:
    JoinOrderEnumerator(const catalog::Catalog& catalog, QueryPlanner* queryPlanner)
        : catalog{catalog},
          queryPlanner{queryPlanner}, context{std::make_unique<JoinOrderEnumeratorContext>()} {};

    std::vector<std::unique_ptr<LogicalPlan>> enumerate(
        const QueryGraphCollection& queryGraphCollection, binder::expression_vector& predicates);

    inline void resetState() { context->resetState(); }

    std::unique_ptr<JoinOrderEnumeratorContext> enterSubquery(
        binder::expression_vector nodeIDsToScanFromInnerAndOuter);
    void exitSubquery(std::unique_ptr<JoinOrderEnumeratorContext> prevContext);

    inline void planMarkJoin(const binder::expression_vector& joinNodeIDs,
        std::shared_ptr<Expression> mark, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
        planJoin(joinNodeIDs, common::JoinType::MARK, mark, probePlan, buildPlan);
    }
    inline void planInnerHashJoin(const binder::expression_vector& joinNodeIDs,
        LogicalPlan& probePlan, LogicalPlan& buildPlan) {
        planJoin(joinNodeIDs, common::JoinType::INNER, nullptr /* mark */, probePlan, buildPlan);
    }
    inline void planLeftHashJoin(const binder::expression_vector& joinNodeIDs,
        LogicalPlan& probePlan, LogicalPlan& buildPlan) {
        planJoin(joinNodeIDs, common::JoinType::LEFT, nullptr /* mark */, probePlan, buildPlan);
    }
    inline void planCrossProduct(LogicalPlan& probePlan, LogicalPlan& buildPlan) {
        appendCrossProduct(probePlan, buildPlan);
    }

private:
    std::vector<std::unique_ptr<LogicalPlan>> planCrossProduct(
        std::vector<std::unique_ptr<LogicalPlan>> leftPlans,
        std::vector<std::unique_ptr<LogicalPlan>> rightPlans);

    std::vector<std::unique_ptr<LogicalPlan>> enumerate(
        QueryGraph* queryGraph, binder::expression_vector& predicates);

    void planBaseTableScan();
    void planNodeScan(uint32_t nodePos);
    void planRelScan(uint32_t relPos);
    void appendExtendAndFilter(std::shared_ptr<NodeExpression> boundNode,
        std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
        ExtendDirection direction, const expression_vector& predicates, LogicalPlan& plan);

    void planLevel(uint32_t level);
    void planLevelExactly(uint32_t level);
    void planLevelApproximately(uint32_t level);

    void planWCOJoin(uint32_t leftLevel, uint32_t rightLevel);
    void planWCOJoin(const SubqueryGraph& subgraph,
        std::vector<std::shared_ptr<RelExpression>> rels,
        const std::shared_ptr<NodeExpression>& intersectNode);

    void planInnerJoin(uint32_t leftLevel, uint32_t rightLevel);

    bool tryPlanINLJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        const std::vector<std::shared_ptr<NodeExpression>>& joinNodes);
    void planInnerHashJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        std::vector<std::shared_ptr<NodeExpression>> joinNodes, bool flipPlan);
    // Filter push down for hash join.
    void planFiltersForHashJoin(binder::expression_vector& predicates, LogicalPlan& plan);

    void appendScanNodeID(std::shared_ptr<NodeExpression>& node, LogicalPlan& plan);

    void appendNonRecursiveExtend(std::shared_ptr<NodeExpression> boundNode,
        std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
        ExtendDirection direction, const binder::expression_vector& properties, LogicalPlan& plan);
    void appendRecursiveExtend(std::shared_ptr<NodeExpression> boundNode,
        std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
        ExtendDirection direction, LogicalPlan& plan);
    void createRecursivePlan(std::shared_ptr<NodeExpression> boundNode,
        std::shared_ptr<NodeExpression> recursiveNode, std::shared_ptr<RelExpression> recursiveRel,
        ExtendDirection direction, LogicalPlan& plan);
    void createPathNodePropertyScanPlan(
        std::shared_ptr<NodeExpression> recursiveNode, LogicalPlan& plan);
    void createPathRelPropertyScanPlan(std::shared_ptr<NodeExpression> recursiveNode,
        std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> recursiveRel,
        ExtendDirection direction, LogicalPlan& plan);

    void planJoin(const binder::expression_vector& joinNodeIDs, common::JoinType joinType,
        std::shared_ptr<Expression> mark, LogicalPlan& probePlan, LogicalPlan& buildPlan);
    void appendHashJoin(const binder::expression_vector& joinNodeIDs, common::JoinType joinType,
        LogicalPlan& probePlan, LogicalPlan& buildPlan);
    void appendMarkJoin(const binder::expression_vector& joinNodeIDs,
        const std::shared_ptr<Expression>& mark, LogicalPlan& probePlan, LogicalPlan& buildPlan);
    void appendIntersect(const std::shared_ptr<Expression>& intersectNodeID,
        binder::expression_vector& boundNodeIDs, LogicalPlan& probePlan,
        std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);
    void appendCrossProduct(LogicalPlan& probePlan, LogicalPlan& buildPlan);

    static binder::expression_vector getNewlyMatchedExpressions(const SubqueryGraph& prevSubgraph,
        const SubqueryGraph& newSubgraph, const binder::expression_vector& expressions) {
        return getNewlyMatchedExpressions(
            std::vector<SubqueryGraph>{prevSubgraph}, newSubgraph, expressions);
    }
    static binder::expression_vector getNewlyMatchedExpressions(
        const std::vector<SubqueryGraph>& prevSubgraphs, const SubqueryGraph& newSubgraph,
        const binder::expression_vector& expressions);
    static bool isExpressionNewlyMatched(const std::vector<SubqueryGraph>& prevSubgraphs,
        const SubqueryGraph& newSubgraph, Expression& expression);

private:
    const catalog::Catalog& catalog;
    QueryPlanner* queryPlanner;
    std::unique_ptr<JoinOrderEnumeratorContext> context;
};

} // namespace planner
} // namespace kuzu
