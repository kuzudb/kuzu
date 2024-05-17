#pragma once

#include "binder/bound_statement.h"
#include "binder/query/query_graph.h"
#include "common/enums/join_type.h"
#include "planner/join_order/cardinality_estimator.h"
#include "planner/join_order_enumerator_context.h"
#include "planner/operator/extend/extend_direction.h"
#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace catalog {
class Catalog;
}

namespace binder {
struct BoundFileScanInfo;
struct BoundCopyFromInfo;
struct BoundInsertInfo;
struct BoundSetPropertyInfo;
struct BoundDeleteInfo;
class NormalizedSingleQuery;
class NormalizedQueryPart;
class BoundReadingClause;
class BoundUpdatingClause;
class BoundProjectionBody;
} // namespace binder
namespace planner {

struct LogicalInsertInfo;
struct LogicalSetPropertyInfo;

class Planner {
public:
    explicit Planner(main::ClientContext* clientContext);
    DELETE_COPY_AND_MOVE(Planner);

    std::unique_ptr<LogicalPlan> getBestPlan(const binder::BoundStatement& statement);

    std::vector<std::unique_ptr<LogicalPlan>> getAllPlans(const binder::BoundStatement& statement);

private:
    // Plan simple statement.
    void appendCreateTable(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendCreateSequence(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendDropTable(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendDropSequence(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendAlter(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendStandaloneCall(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendCommentOn(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendExplain(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendCreateMacro(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendTransaction(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendExtension(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendAttachDatabase(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendDetachDatabase(const binder::BoundStatement& statement, LogicalPlan& plan);
    void appendUseDatabase(const binder::BoundStatement& statement, LogicalPlan& plan);

    // Plan copy.
    std::unique_ptr<LogicalPlan> planCopyTo(const binder::BoundStatement& statement);
    std::unique_ptr<LogicalPlan> planCopyFrom(const binder::BoundStatement& statement);
    std::unique_ptr<LogicalPlan> planCopyNodeFrom(const binder::BoundCopyFromInfo* info,
        binder::expression_vector outExprs);
    std::unique_ptr<LogicalPlan> planCopyResourceFrom(const binder::BoundCopyFromInfo* info,
        binder::expression_vector results);
    std::unique_ptr<LogicalPlan> planCopyRelFrom(const binder::BoundCopyFromInfo* info,
        binder::expression_vector outExprs);
    std::unique_ptr<LogicalPlan> planCopyRdfFrom(const binder::BoundCopyFromInfo* info,
        binder::expression_vector outExprs);

    // Plan export/import database
    std::unique_ptr<LogicalPlan> planExportDatabase(const binder::BoundStatement& statement);
    std::unique_ptr<LogicalPlan> planImportDatabase(const binder::BoundStatement& statement);

    // Plan query.
    std::vector<std::unique_ptr<LogicalPlan>> planQuery(
        const binder::BoundStatement& boundStatement);
    std::vector<std::unique_ptr<LogicalPlan>> planSingleQuery(
        const binder::NormalizedSingleQuery* singleQuery);
    std::vector<std::unique_ptr<LogicalPlan>> planQueryPart(
        const binder::NormalizedQueryPart* queryPart,
        std::vector<std::unique_ptr<LogicalPlan>> prevPlans);

    // Plan read.
    void planReadingClause(const binder::BoundReadingClause* readingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& prevPlans);
    void planMatchClause(const binder::BoundReadingClause* readingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planUnwindClause(const binder::BoundReadingClause* readingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planInQueryCall(const binder::BoundReadingClause* readingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planLoadFrom(const binder::BoundReadingClause* readingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& plans);

    // Plan updating
    void planUpdatingClause(const binder::BoundUpdatingClause* updatingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planUpdatingClause(const binder::BoundUpdatingClause* updatingClause, LogicalPlan& plan);
    void planInsertClause(const binder::BoundUpdatingClause* updatingClause, LogicalPlan& plan);
    void planMergeClause(const binder::BoundUpdatingClause* updatingClause, LogicalPlan& plan);
    void planSetClause(const binder::BoundUpdatingClause* updatingClause, LogicalPlan& plan);
    void planDeleteClause(const binder::BoundUpdatingClause* updatingClause, LogicalPlan& plan);

    // Plan projection
    void planProjectionBody(const binder::BoundProjectionBody* projectionBody,
        const std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planProjectionBody(const binder::BoundProjectionBody* projectionBody, LogicalPlan& plan);
    void planAggregate(const binder::expression_vector& expressionsToAggregate,
        const binder::expression_vector& expressionsToGroupBy, LogicalPlan& plan);
    void planOrderBy(const binder::expression_vector& expressionsToProject,
        const binder::expression_vector& expressionsToOrderBy, const std::vector<bool>& isAscOrders,
        LogicalPlan& plan);

    // Plan subquery
    void planOptionalMatch(const binder::QueryGraphCollection& queryGraphCollection,
        const binder::expression_vector& predicates, const binder::expression_vector& corrExprs,
        LogicalPlan& leftPlan);
    // Write whether optional match succeed or not to mark.
    void planOptionalMatch(const binder::QueryGraphCollection& queryGraphCollection,
        const binder::expression_vector& predicates, const binder::expression_vector& corrExprs,
        std::shared_ptr<binder::Expression> mark, LogicalPlan& leftPlan);
    void planRegularMatch(const binder::QueryGraphCollection& queryGraphCollection,
        const binder::expression_vector& predicates, LogicalPlan& leftPlan);
    void planSubquery(const std::shared_ptr<binder::Expression>& subquery, LogicalPlan& outerPlan);
    void planSubqueryIfNecessary(const std::shared_ptr<binder::Expression>& expression,
        LogicalPlan& plan);

    static binder::expression_vector getCorrelatedExprs(
        const binder::QueryGraphCollection& collection, const binder::expression_vector& predicates,
        Schema* outerSchema);

    // Plan query graphs
    std::unique_ptr<LogicalPlan> planQueryGraphCollection(
        const binder::QueryGraphCollection& queryGraphCollection,
        const binder::expression_vector& predicates);
    std::unique_ptr<LogicalPlan> planQueryGraphCollectionInNewContext(SubqueryType subqueryType,
        const binder::expression_vector& correlatedExpressions, uint64_t cardinality,
        const binder::QueryGraphCollection& queryGraphCollection,
        const binder::expression_vector& predicates);
    std::vector<std::unique_ptr<LogicalPlan>> enumerateQueryGraphCollection(
        const binder::QueryGraphCollection& queryGraphCollection,
        const binder::expression_vector& predicates);
    std::vector<std::unique_ptr<LogicalPlan>> enumerateQueryGraph(SubqueryType subqueryType,
        const binder::expression_vector& correlatedExpressions,
        const binder::QueryGraph& queryGraph, binder::expression_vector& predicates);

    // Plan node/rel table scan
    void planBaseTableScans(SubqueryType subqueryType,
        const binder::expression_vector& correlatedExpressions);
    void planCorrelatedExpressionsScan(const binder::expression_vector& correlatedExpressions);
    void planNodeScan(uint32_t nodePos);
    void planNodeIDScan(uint32_t nodePos);
    void planRelScan(uint32_t relPos);
    void appendExtendAndFilter(const std::shared_ptr<binder::NodeExpression>& boundNode,
        const std::shared_ptr<binder::NodeExpression>& nbrNode,
        const std::shared_ptr<binder::RelExpression>& rel, ExtendDirection direction,
        const binder::expression_vector& predicates, LogicalPlan& plan);

    // Plan dp level
    void planLevel(uint32_t level);
    void planLevelExactly(uint32_t level);
    void planLevelApproximately(uint32_t level);

    // Plan worst case optimal join
    void planWCOJoin(uint32_t leftLevel, uint32_t rightLevel);
    void planWCOJoin(const binder::SubqueryGraph& subgraph,
        const std::vector<std::shared_ptr<binder::RelExpression>>& rels,
        const std::shared_ptr<binder::NodeExpression>& intersectNode);

    // Plan index-nested-loop join / hash join
    void planInnerJoin(uint32_t leftLevel, uint32_t rightLevel);
    bool tryPlanINLJoin(const binder::SubqueryGraph& subgraph,
        const binder::SubqueryGraph& otherSubgraph,
        const std::vector<std::shared_ptr<binder::NodeExpression>>& joinNodes);
    void planInnerHashJoin(const binder::SubqueryGraph& subgraph,
        const binder::SubqueryGraph& otherSubgraph,
        const std::vector<std::shared_ptr<binder::NodeExpression>>& joinNodes, bool flipPlan);

    std::vector<std::unique_ptr<LogicalPlan>> planCrossProduct(
        std::vector<std::unique_ptr<LogicalPlan>> leftPlans,
        std::vector<std::unique_ptr<LogicalPlan>> rightPlans);

    // Append empty result
    void appendEmptyResult(LogicalPlan& plan);

    // Append updating operators
    void appendInsertNode(const std::vector<const binder::BoundInsertInfo*>& boundInsertInfos,
        LogicalPlan& plan);
    void appendInsertRel(const std::vector<const binder::BoundInsertInfo*>& boundInsertInfos,
        LogicalPlan& plan);
    void appendSetNodeProperty(const std::vector<const binder::BoundSetPropertyInfo*>& boundInfos,
        LogicalPlan& plan);
    void appendSetRelProperty(const std::vector<const binder::BoundSetPropertyInfo*>& boundInfos,
        LogicalPlan& plan);
    void appendDelete(const std::vector<binder::BoundDeleteInfo>& infos, LogicalPlan& plan);
    std::unique_ptr<LogicalInsertInfo> createLogicalInsertInfo(const binder::BoundInsertInfo* info);
    std::unique_ptr<LogicalSetPropertyInfo> createLogicalSetPropertyInfo(
        const binder::BoundSetPropertyInfo* boundSetPropertyInfo);

    // Append projection operators
    void appendProjection(const binder::expression_vector& expressionsToProject, LogicalPlan& plan);
    void appendAggregate(const binder::expression_vector& expressionsToGroupBy,
        const binder::expression_vector& expressionsToAggregate, LogicalPlan& plan);
    void appendOrderBy(const binder::expression_vector& expressions,
        const std::vector<bool>& isAscOrders, LogicalPlan& plan);
    void appendMultiplicityReducer(LogicalPlan& plan);
    void appendLimit(uint64_t skipNum, uint64_t limitNum, LogicalPlan& plan);

    // Append scan operators
    void appendExpressionsScan(const binder::expression_vector& expressions, LogicalPlan& plan);
    void appendScanInternalID(std::shared_ptr<binder::Expression> internalID,
        std::vector<common::table_id_t> tableIDs, LogicalPlan& plan);
    void appendScanNodeProperties(std::shared_ptr<binder::Expression> nodeID,
        std::vector<common::table_id_t> tableIDs, const binder::expression_vector& properties,
        LogicalPlan& plan);

    // Append extend operators
    void appendNonRecursiveExtend(const std::shared_ptr<binder::NodeExpression>& boundNode,
        const std::shared_ptr<binder::NodeExpression>& nbrNode,
        const std::shared_ptr<binder::RelExpression>& rel, ExtendDirection direction,
        const binder::expression_vector& properties, LogicalPlan& plan);
    void appendRecursiveExtend(const std::shared_ptr<binder::NodeExpression>& boundNode,
        const std::shared_ptr<binder::NodeExpression>& nbrNode,
        const std::shared_ptr<binder::RelExpression>& rel, ExtendDirection direction,
        LogicalPlan& plan);
    void createRecursivePlan(const binder::RecursiveInfo& recursiveInfo, ExtendDirection direction,
        LogicalPlan& plan);
    void createPathNodePropertyScanPlan(const std::shared_ptr<binder::NodeExpression>& node,
        const binder::expression_vector& properties, LogicalPlan& plan);
    void createPathRelPropertyScanPlan(const std::shared_ptr<binder::NodeExpression>& boundNode,
        const std::shared_ptr<binder::NodeExpression>& nbrNode,
        const std::shared_ptr<binder::RelExpression>& recursiveRel, ExtendDirection direction,
        const binder::expression_vector& properties, LogicalPlan& plan);
    void appendNodeLabelFilter(std::shared_ptr<binder::Expression> nodeID,
        std::unordered_set<common::table_id_t> tableIDSet, LogicalPlan& plan);

    // Append Join operators
    void appendHashJoin(const binder::expression_vector& joinNodeIDs, common::JoinType joinType,
        LogicalPlan& probePlan, LogicalPlan& buildPlan, LogicalPlan& resultPlan);
    void appendHashJoin(const binder::expression_vector& joinNodeIDs, common::JoinType joinType,
        std::shared_ptr<binder::Expression> mark, LogicalPlan& probePlan, LogicalPlan& buildPlan,
        LogicalPlan& resultPlan);
    void appendMarkJoin(const binder::expression_vector& joinNodeIDs,
        const std::shared_ptr<binder::Expression>& mark, LogicalPlan& probePlan,
        LogicalPlan& buildPlan);
    void appendIntersect(const std::shared_ptr<binder::Expression>& intersectNodeID,
        binder::expression_vector& boundNodeIDs, LogicalPlan& probePlan,
        std::vector<std::unique_ptr<LogicalPlan>>& buildPlans);

    void appendCrossProduct(common::AccumulateType accumulateType, const LogicalPlan& probePlan,
        const LogicalPlan& buildPlan, LogicalPlan& resultPlan);

    /* Append accumulate */

    // Accumulate everything.
    void appendAccumulate(LogicalPlan& plan);
    // Accumulate everything. Append
    void appendOptionalAccumulate(std::shared_ptr<binder::Expression> mark, LogicalPlan& plan);
    // Append accumulate with a set of expressions being flattened first.
    void appendAccumulate(const binder::expression_vector& flatExprs, LogicalPlan& plan);
    // Append accumulate with a set of expressions being flattened first.
    // Additionally, scan table with row offset.
    void appendAccumulate(common::AccumulateType accumulateType,
        const binder::expression_vector& flatExprs, std::shared_ptr<binder::Expression> offset,
        std::shared_ptr<binder::Expression> mark, LogicalPlan& plan);
    void appendMarkAccumulate(const binder::expression_vector& keys,
        std::shared_ptr<binder::Expression> mark, LogicalPlan& plan);

    void appendDummyScan(LogicalPlan& plan);

    void appendUnwind(const binder::BoundReadingClause& boundReadingClause, LogicalPlan& plan);

    void appendInQueryCall(const binder::BoundReadingClause& boundReadingClause, LogicalPlan& plan);

    void appendFlattens(const f_group_pos_set& groupsPos, LogicalPlan& plan);
    void appendFlattenIfNecessary(f_group_pos groupPos, LogicalPlan& plan);

    void appendFilters(const binder::expression_vector& predicates, LogicalPlan& plan);
    void appendFilter(const std::shared_ptr<binder::Expression>& predicate, LogicalPlan& plan);

    // Append scan file.
    void appendScanFile(const binder::BoundFileScanInfo* info, LogicalPlan& plan);
    // Append scan file. Additionally, scan row offset.
    void appendScanFile(const binder::BoundFileScanInfo* info,
        std::shared_ptr<binder::Expression> offset, LogicalPlan& plan);

    void appendDistinct(const binder::expression_vector& keys, LogicalPlan& plan);

    std::unique_ptr<LogicalPlan> createUnionPlan(
        std::vector<std::unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll);
    std::unique_ptr<LogicalPlan> getBestPlan(std::vector<std::unique_ptr<LogicalPlan>> plans);

    static std::vector<std::unique_ptr<LogicalPlan>> getInitialEmptyPlans();

    binder::expression_vector getProperties(const binder::Expression& nodeOrRel);

    JoinOrderEnumeratorContext enterContext(SubqueryType subqueryType,
        const binder::expression_vector& correlatedExpressions, uint64_t cardinality);
    void exitContext(JoinOrderEnumeratorContext prevContext);

private:
    main::ClientContext* clientContext;
    binder::expression_vector propertiesToScan;
    CardinalityEstimator cardinalityEstimator;
    JoinOrderEnumeratorContext context;
};

} // namespace planner
} // namespace kuzu
