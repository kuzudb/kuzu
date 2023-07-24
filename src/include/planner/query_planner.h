#pragma once

#include "binder/bound_statement.h"
#include "binder/call/bound_in_query_call.h"
#include "binder/expression/existential_subquery_expression.h"
#include "join_order_enumerator.h"
#include "planner/join_order/cardinality_estimator.h"
#include "projection_planner.h"

namespace kuzu {
namespace binder {
class BoundCreateNode;
class BoundCreateRel;
class BoundSetNodeProperty;
class BoundSetRelProperty;
class BoundDeleteNode;
} // namespace binder

namespace planner {

class QueryPlanner {
    friend class JoinOrderEnumerator;
    friend class ProjectionPlanner;

public:
    explicit QueryPlanner(const catalog::Catalog& catalog,
        const storage::NodesStatisticsAndDeletedIDs& nodesStatistics,
        const storage::RelsStatistics& relsStatistics)
        : catalog{catalog}, cardinalityEstimator{std::make_unique<CardinalityEstimator>(
                                nodesStatistics, relsStatistics)},
          joinOrderEnumerator{catalog, this}, projectionPlanner{this} {}

    std::vector<std::unique_ptr<LogicalPlan>> getAllPlans(const BoundStatement& boundStatement);

    inline std::unique_ptr<LogicalPlan> getBestPlan(const BoundStatement& boundStatement) {
        return getBestPlan(getAllPlans(boundStatement));
    }

private:
    std::unique_ptr<LogicalPlan> getBestPlan(std::vector<std::unique_ptr<LogicalPlan>> plans);

    // Plan query
    std::vector<std::unique_ptr<LogicalPlan>> planSingleQuery(
        const NormalizedSingleQuery& singleQuery);
    std::vector<std::unique_ptr<LogicalPlan>> planQueryPart(
        const NormalizedQueryPart& queryPart, std::vector<std::unique_ptr<LogicalPlan>> prevPlans);

    // Plan reading
    void planReadingClause(
        BoundReadingClause* readingClause, std::vector<std::unique_ptr<LogicalPlan>>& prevPlans);
    void planMatchClause(
        BoundReadingClause* readingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planUnwindClause(
        BoundReadingClause* readingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planInQueryCall(
        BoundReadingClause* readingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans);

    // Plan updating
    void planUpdatingClause(binder::BoundUpdatingClause& updatingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planUpdatingClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan);
    void planCreateClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan);
    void planSetClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan);
    void planDeleteClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan);

    // Plan subquery
    void planOptionalMatch(const QueryGraphCollection& queryGraphCollection,
        const expression_vector& predicates, LogicalPlan& leftPlan);
    void planRegularMatch(const QueryGraphCollection& queryGraphCollection,
        const expression_vector& predicates, LogicalPlan& leftPlan);
    void planExistsSubquery(std::shared_ptr<Expression> subquery, LogicalPlan& outerPlan);
    void planSubqueryIfNecessary(const std::shared_ptr<Expression>& expression, LogicalPlan& plan);

    std::unique_ptr<LogicalPlan> planJoins(
        const QueryGraphCollection& queryGraphCollection, const expression_vector& predicates);
    std::unique_ptr<LogicalPlan> planJoinsInNewContext(
        const expression_vector& expressionsToExcludeScan,
        const QueryGraphCollection& queryGraphCollection, const expression_vector& predicates);

    void appendAccumulate(common::AccumulateType accumulateType, LogicalPlan& plan);

    void appendExpressionsScan(const expression_vector& expressions, LogicalPlan& plan);

    void appendDistinct(const expression_vector& expressionsToDistinct, LogicalPlan& plan);

    void appendUnwind(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan);

    void appendInQueryCall(BoundInQueryCall& boundInQueryCall, LogicalPlan& plan);

    void appendFlattens(const f_group_pos_set& groupsPos, LogicalPlan& plan);
    void appendFlattenIfNecessary(f_group_pos groupPos, LogicalPlan& plan);

    void appendFilters(const binder::expression_vector& predicates, LogicalPlan& plan);
    void appendFilter(const std::shared_ptr<Expression>& predicate, LogicalPlan& plan);

    void appendScanNodePropIfNecessary(const expression_vector& propertyExpressions,
        std::shared_ptr<NodeExpression> node, LogicalPlan& plan);

    // Append updating operators
    void appendCreateNode(const std::vector<std::unique_ptr<binder::BoundCreateNode>>& createNodes,
        LogicalPlan& plan);
    void appendCreateRel(
        const std::vector<std::unique_ptr<binder::BoundCreateRel>>& createRels, LogicalPlan& plan);
    void appendSetNodeProperty(
        const std::vector<std::unique_ptr<binder::BoundSetNodeProperty>>& setNodeProperties,
        LogicalPlan& plan);
    void appendSetRelProperty(
        const std::vector<std::unique_ptr<binder::BoundSetRelProperty>>& setRelProperties,
        LogicalPlan& plan);
    void appendDeleteNode(const std::vector<std::unique_ptr<binder::BoundDeleteNode>>& deleteNodes,
        LogicalPlan& plan);
    void appendDeleteRel(
        const std::vector<std::shared_ptr<binder::RelExpression>>& deleteRels, LogicalPlan& plan);

    std::unique_ptr<LogicalPlan> createUnionPlan(
        std::vector<std::unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll);

    static std::vector<std::unique_ptr<LogicalPlan>> getInitialEmptyPlans();

    expression_vector getPropertiesForNode(NodeExpression& node);
    expression_vector getPropertiesForRel(RelExpression& rel);

    static std::vector<std::vector<std::unique_ptr<LogicalPlan>>> cartesianProductChildrenPlans(
        std::vector<std::vector<std::unique_ptr<LogicalPlan>>> childrenLogicalPlans);

private:
    const catalog::Catalog& catalog;
    std::unique_ptr<CardinalityEstimator> cardinalityEstimator;
    expression_vector propertiesToScan;
    JoinOrderEnumerator joinOrderEnumerator;
    ProjectionPlanner projectionPlanner;
};

} // namespace planner
} // namespace kuzu
