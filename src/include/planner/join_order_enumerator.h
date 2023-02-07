#pragma once

#include "binder/query/normalized_single_query.h"
#include "catalog/catalog.h"
#include "common/join_type.h"
#include "planner/join_order_enumerator_context.h"
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
    friend class ASPOptimizer;

public:
    JoinOrderEnumerator(const catalog::Catalog& catalog,
        const storage::NodesStatisticsAndDeletedIDs& nodesStatistics,
        const storage::RelsStatistics& relsStatistics, QueryPlanner* queryPlanner)
        : catalog{catalog}, nodesStatistics{nodesStatistics}, relsStatistics{relsStatistics},
          queryPlanner{queryPlanner}, context{std::make_unique<JoinOrderEnumeratorContext>()} {};

    std::vector<std::unique_ptr<LogicalPlan>> enumerate(
        const QueryGraphCollection& queryGraphCollection, binder::expression_vector& predicates);

    inline void resetState() { context->resetState(); }

    std::unique_ptr<JoinOrderEnumeratorContext> enterSubquery(LogicalPlan* outerPlan,
        binder::expression_vector expressionsToScan,
        binder::expression_vector nodeIDsToScanFromInnerAndOuter);
    void exitSubquery(std::unique_ptr<JoinOrderEnumeratorContext> prevContext);

    static inline void planMarkJoin(const binder::expression_vector& joinNodeIDs,
        std::shared_ptr<Expression> mark, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
        planJoin(joinNodeIDs, common::JoinType::MARK, mark, probePlan, buildPlan);
    }
    static inline void planInnerHashJoin(
        const std::vector<std::shared_ptr<NodeExpression>>& joinNodes, LogicalPlan& probePlan,
        LogicalPlan& buildPlan) {
        binder::expression_vector joinNodeIDs;
        for (auto& joinNode : joinNodes) {
            joinNodeIDs.push_back(joinNode->getInternalIDProperty());
        }
        planJoin(joinNodeIDs, common::JoinType::INNER, nullptr /* mark */, probePlan, buildPlan);
    }
    static inline void planInnerHashJoin(const binder::expression_vector& joinNodeIDs,
        LogicalPlan& probePlan, LogicalPlan& buildPlan) {
        planJoin(joinNodeIDs, common::JoinType::INNER, nullptr /* mark */, probePlan, buildPlan);
    }
    static inline void planLeftHashJoin(const binder::expression_vector& joinNodeIDs,
        LogicalPlan& probePlan, LogicalPlan& buildPlan) {
        planJoin(joinNodeIDs, common::JoinType::LEFT, nullptr /* mark */, probePlan, buildPlan);
    }
    static inline void planCrossProduct(LogicalPlan& probePlan, LogicalPlan& buildPlan) {
        appendCrossProduct(probePlan, buildPlan);
    }

private:
    std::vector<std::unique_ptr<LogicalPlan>> planCrossProduct(
        std::vector<std::unique_ptr<LogicalPlan>> leftPlans,
        std::vector<std::unique_ptr<LogicalPlan>> rightPlans);

    std::vector<std::unique_ptr<LogicalPlan>> enumerate(
        QueryGraph* queryGraph, binder::expression_vector& predicates);

    void planOuterExpressionsScan(binder::expression_vector& expressions);

    void planTableScan();

    void planNodeScan(uint32_t nodePos);
    // Filter push down for node table.
    void planFiltersForNode(binder::expression_vector& predicates,
        std::shared_ptr<NodeExpression> node, LogicalPlan& plan);
    // Property push down for node table.
    void planPropertyScansForNode(std::shared_ptr<NodeExpression> node, LogicalPlan& plan);

    void planRelScan(uint32_t relPos);

    void planExtendAndFilters(std::shared_ptr<RelExpression> rel, common::RelDirection direction,
        binder::expression_vector& predicates, LogicalPlan& plan);

    void planLevel(uint32_t level);
    void planLevelExactly(uint32_t level);
    void planLevelApproximately(uint32_t level);

    void planWCOJoin(uint32_t leftLevel, uint32_t rightLevel);
    void planWCOJoin(const SubqueryGraph& subgraph,
        std::vector<std::shared_ptr<RelExpression>> rels,
        const std::shared_ptr<NodeExpression>& intersectNode);

    void planInnerJoin(uint32_t leftLevel, uint32_t rightLevel);

    bool canApplyINLJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        const std::vector<std::shared_ptr<NodeExpression>>& joinNodes);
    void planInnerINLJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        const std::vector<std::shared_ptr<NodeExpression>>& joinNodes);
    void planInnerHashJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        std::vector<std::shared_ptr<NodeExpression>> joinNodes, bool flipPlan);
    // Filter push down for hash join.
    void planFiltersForHashJoin(binder::expression_vector& predicates, LogicalPlan& plan);

    void appendFTableScan(
        LogicalPlan* outerPlan, binder::expression_vector& expressionsToScan, LogicalPlan& plan);

    void appendScanNode(std::shared_ptr<NodeExpression>& node, LogicalPlan& plan);
    void appendIndexScanNode(std::shared_ptr<NodeExpression>& node,
        std::shared_ptr<Expression> indexExpression, LogicalPlan& plan);

    bool needFlatInput(
        RelExpression& rel, NodeExpression& boundNode, common::RelDirection direction);
    bool needExtendToNewGroup(
        RelExpression& rel, NodeExpression& boundNode, common::RelDirection direction);
    void appendExtend(std::shared_ptr<NodeExpression> boundNode,
        std::shared_ptr<NodeExpression> nbrNode, std::shared_ptr<RelExpression> rel,
        common::RelDirection direction, const binder::expression_vector& properties,
        LogicalPlan& plan);

    static void planJoin(const binder::expression_vector& joinNodeIDs, common::JoinType joinType,
        std::shared_ptr<Expression> mark, LogicalPlan& probePlan, LogicalPlan& buildPlan);
    static void appendHashJoin(const binder::expression_vector& joinNodeIDs,
        common::JoinType joinType, bool isProbeAcc, LogicalPlan& probePlan, LogicalPlan& buildPlan);
    static void appendMarkJoin(const binder::expression_vector& joinNodeIDs,
        const std::shared_ptr<Expression>& mark, bool isProbeAcc, LogicalPlan& probePlan,
        LogicalPlan& buildPlan);
    static void appendIntersect(const std::shared_ptr<NodeExpression>& intersectNode,
        std::vector<std::shared_ptr<NodeExpression>>& boundNodes, LogicalPlan& probePlan,
        std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);
    static void appendCrossProduct(LogicalPlan& probePlan, LogicalPlan& buildPlan);

    binder::expression_vector getPropertiesForVariable(
        Expression& expression, Expression& variable);
    uint64_t getExtensionRate(
        const RelExpression& rel, const NodeExpression& boundNode, common::RelDirection direction);

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
    const storage::NodesStatisticsAndDeletedIDs& nodesStatistics;
    const storage::RelsStatistics& relsStatistics;
    QueryPlanner* queryPlanner;
    std::unique_ptr<JoinOrderEnumeratorContext> context;
};

} // namespace planner
} // namespace kuzu
