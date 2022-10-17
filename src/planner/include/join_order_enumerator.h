#pragma once

#include "src/binder/query/include/normalized_single_query.h"
#include "src/catalog/include/catalog.h"
#include "src/common/include/join_type.h"
#include "src/planner/include/join_order_enumerator_context.h"
#include "src/storage/store/include/nodes_statistics_and_deleted_ids.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace planner {

class Enumerator;
class JoinOrderEnumeratorContext;

/**
 * JoinOrderEnumerator is currently responsible for
 *      join order enumeration
 *      filter push down
 *      property push down
 */
class JoinOrderEnumerator {
    friend class Enumerator;
    friend class ASPOptimizer;

public:
    JoinOrderEnumerator(const Catalog& catalog,
        const NodesStatisticsAndDeletedIDs& nodesStatisticsAndDeletedIDs,
        const RelsStatistics& relsStatistics, Enumerator* enumerator)
        : catalog{catalog}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs},
          relsStatistics{relsStatistics},
          enumerator{enumerator}, context{make_unique<JoinOrderEnumeratorContext>()} {};

    vector<unique_ptr<LogicalPlan>> enumerateJoinOrder(const QueryGraph& queryGraph,
        const shared_ptr<Expression>& queryGraphPredicate,
        vector<unique_ptr<LogicalPlan>> prevPlans);

    inline void resetState() { context->resetState(); }

private:
    unique_ptr<JoinOrderEnumeratorContext> enterSubquery(LogicalPlan* outerPlan,
        expression_vector expressionsToScan, vector<shared_ptr<NodeExpression>> nodesToScanTwice);
    void exitSubquery(unique_ptr<JoinOrderEnumeratorContext> prevContext);

    void planOuterExpressionsScan(expression_vector& expressions);
    inline void planTableScan() {
        planNodeScan();
        planRelScan();
    }

    void planNodeScan();
    // Filter push down for node table.
    void planFiltersForNode(expression_vector& predicates, NodeExpression& node, LogicalPlan& plan);
    // Property push down for node table.
    void planPropertyScansForNode(NodeExpression& node, LogicalPlan& plan);

    void planRelScan();
    inline void planRelExtendFiltersAndProperties(shared_ptr<RelExpression>& rel,
        RelDirection direction, expression_vector& predicates, LogicalPlan& plan) {
        appendExtend(rel, direction, plan);
        planFiltersForRel(predicates, *rel, direction, plan);
        planPropertyScansForRel(*rel, direction, plan);
    }
    // Filter push down for rel table.
    void planFiltersForRel(expression_vector& predicates, RelExpression& rel,
        RelDirection direction, LogicalPlan& plan);
    // Property push down for rel table.
    void planPropertyScansForRel(RelExpression& rel, RelDirection direction, LogicalPlan& plan);

    inline void planLevel(uint32_t level) {
        assert(level > 1);
        auto maxLeftLevel = floor(level / 2.0);
        for (auto leftLevel = 1u; leftLevel <= maxLeftLevel; ++leftLevel) {
            auto rightLevel = level - leftLevel;
            //            if (leftLevel > 1) { // wcoj requires at least 2 rels
            //                planWCOJoin(leftLevel, rightLevel);
            //            }
            planInnerJoin(leftLevel, rightLevel);
        }
        context->subPlansTable->finalizeLevel(level);
    }

    void planWCOJoin(uint32_t leftLevel, uint32_t rightLevel);
    void planWCOJoin(const SubqueryGraph& subgraph, vector<shared_ptr<RelExpression>> rels,
        shared_ptr<NodeExpression> intersectNode);

    void planInnerJoin(uint32_t leftLevel, uint32_t rightLevel);

    bool canApplyINLJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        const vector<shared_ptr<NodeExpression>>& joinNodes);
    void planInnerINLJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        const vector<shared_ptr<NodeExpression>>& joinNodes);
    void planInnerHashJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        vector<shared_ptr<NodeExpression>> joinNodes, bool flipPlan);
    // Filter push down for hash join.
    void planFiltersForHashJoin(expression_vector& predicates, LogicalPlan& plan);

    void appendFTableScan(
        LogicalPlan* outerPlan, expression_vector& expressionsToScan, LogicalPlan& plan);

    void appendScanNodeID(shared_ptr<NodeExpression>& node, LogicalPlan& plan);

    void appendExtend(shared_ptr<RelExpression>& rel, RelDirection direction, LogicalPlan& plan);
    static void planHashJoin(const vector<shared_ptr<NodeExpression>>& joinNodes, JoinType joinType,
        bool isProbeAcc, LogicalPlan& probePlan, LogicalPlan& buildPlan);
    static void appendHashJoin(const vector<shared_ptr<NodeExpression>>& joinNodes,
        JoinType joinType, bool isProbeAcc, LogicalPlan& probePlan, LogicalPlan& buildPlan);
    static void appendMarkJoin(const vector<shared_ptr<NodeExpression>>& joinNodes,
        const shared_ptr<Expression>& mark, LogicalPlan& probePlan, LogicalPlan& buildPlan);
    static void appendIntersect(const shared_ptr<NodeExpression>& intersectNode,
        vector<shared_ptr<NodeExpression>>& boundNodes, LogicalPlan& probePlan,
        vector<unique_ptr<LogicalPlan>>& buildPlans);

    expression_vector getPropertiesForVariable(Expression& expression, Expression& variable);
    uint64_t getExtensionRate(
        table_id_t boundTableID, table_id_t relTableID, RelDirection relDirection);

private:
    const catalog::Catalog& catalog;
    const storage::NodesStatisticsAndDeletedIDs& nodesStatisticsAndDeletedIDs;
    const storage::RelsStatistics& relsStatistics;
    Enumerator* enumerator;
    unique_ptr<JoinOrderEnumeratorContext> context;
};

} // namespace planner
} // namespace graphflow
