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
    unique_ptr<JoinOrderEnumeratorContext> enterSubquery(expression_vector expressionsToScan);
    void exitSubquery(unique_ptr<JoinOrderEnumeratorContext> prevContext);

    void planResultScan();

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
            planInnerJoin(leftLevel, rightLevel);
        }
        context->subPlansTable->finalizeLevel(level);
    }

    void planInnerJoin(uint32_t leftLevel, uint32_t rightLevel);

    bool canApplyINLJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        shared_ptr<NodeExpression> joinNode);
    void planInnerINLJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        shared_ptr<NodeExpression> joinNode);
    void planInnerHashJoin(const SubqueryGraph& subgraph, const SubqueryGraph& otherSubgraph,
        shared_ptr<NodeExpression> joinNode, bool flipPlan);
    // Filter push down for hash join.
    void planFiltersForHashJoin(expression_vector& predicates, LogicalPlan& plan);

    void appendResultScan(const expression_vector& expressionsToSelect, LogicalPlan& plan);
    void appendScanNodeID(shared_ptr<NodeExpression>& node, LogicalPlan& plan);

    void appendExtend(shared_ptr<RelExpression>& rel, RelDirection direction, LogicalPlan& plan);
    void appendHashJoin(const shared_ptr<NodeExpression>& joinNode, JoinType joinType,
        LogicalPlan& probePlan, LogicalPlan& buildPlan);
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
