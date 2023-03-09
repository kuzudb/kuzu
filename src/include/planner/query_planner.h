#pragma once

#include "binder/bound_statement.h"
#include "binder/expression/existential_subquery_expression.h"
#include "join_order_enumerator.h"
#include "projection_planner.h"
#include "update_planner.h"

namespace kuzu {
namespace planner {

class QueryPlanner {
    friend class JoinOrderEnumerator;
    friend class ProjectionPlanner;
    friend class UpdatePlanner;
    friend class ASPOptimizer;

public:
    explicit QueryPlanner(const catalog::Catalog& catalog,
        const storage::NodesStatisticsAndDeletedIDs& nodesStatistics,
        const storage::RelsStatistics& relsStatistics)
        : catalog{catalog}, joinOrderEnumerator{catalog, nodesStatistics, relsStatistics, this},
          projectionPlanner{this}, updatePlanner{} {}

    std::vector<std::unique_ptr<LogicalPlan>> getAllPlans(const BoundStatement& boundStatement);

    inline std::unique_ptr<LogicalPlan> getBestPlan(const BoundStatement& boundStatement) {
        return getBestPlan(getAllPlans(boundStatement));
    }

private:
    std::unique_ptr<LogicalPlan> getBestPlan(std::vector<std::unique_ptr<LogicalPlan>> plans);

    std::vector<std::unique_ptr<LogicalPlan>> planSingleQuery(
        const NormalizedSingleQuery& singleQuery);
    std::vector<std::unique_ptr<LogicalPlan>> planQueryPart(
        const NormalizedQueryPart& queryPart, std::vector<std::unique_ptr<LogicalPlan>> prevPlans);

    // Reading clause planning
    void planReadingClause(BoundReadingClause* boundReadingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& prevPlans);
    void planMatchClause(
        BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans);
    void planUnwindClause(
        BoundReadingClause* boundReadingClause, std::vector<std::unique_ptr<LogicalPlan>>& plans);

    // CTE & subquery planning
    void planOptionalMatch(const QueryGraphCollection& queryGraphCollection,
        expression_vector& predicates, LogicalPlan& outerPlan);
    void planRegularMatch(const QueryGraphCollection& queryGraphCollection,
        expression_vector& predicates, LogicalPlan& prevPlan);
    void planExistsSubquery(std::shared_ptr<Expression>& subquery, LogicalPlan& outerPlan);
    void planSubqueryIfNecessary(const std::shared_ptr<Expression>& expression, LogicalPlan& plan);

    static void appendAccumulate(LogicalPlan& plan);

    static void appendExpressionsScan(const expression_vector& expressions, LogicalPlan& plan);

    static void appendDistinct(const expression_vector& expressionsToDistinct, LogicalPlan& plan);

    static void appendUnwind(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan);

    static void appendFlattens(const f_group_pos_set& groupsPos, LogicalPlan& plan);
    static void appendFlattenIfNecessary(f_group_pos groupPos, LogicalPlan& plan);

    void appendFilter(const std::shared_ptr<Expression>& expression, LogicalPlan& plan);

    void appendScanNodePropIfNecessary(const expression_vector& propertyExpressions,
        std::shared_ptr<NodeExpression> node, LogicalPlan& plan);

    std::unique_ptr<LogicalPlan> createUnionPlan(
        std::vector<std::unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll);

    static std::vector<std::unique_ptr<LogicalPlan>> getInitialEmptyPlans();

    expression_vector getPropertiesForNode(NodeExpression& node);
    expression_vector getPropertiesForRel(RelExpression& rel);

    static std::vector<std::vector<std::unique_ptr<LogicalPlan>>> cartesianProductChildrenPlans(
        std::vector<std::vector<std::unique_ptr<LogicalPlan>>> childrenLogicalPlans);

private:
    const catalog::Catalog& catalog;
    expression_vector propertiesToScan;
    JoinOrderEnumerator joinOrderEnumerator;
    ProjectionPlanner projectionPlanner;
    UpdatePlanner updatePlanner;
};

} // namespace planner
} // namespace kuzu
