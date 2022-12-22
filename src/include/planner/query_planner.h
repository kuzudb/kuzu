#pragma once

#include "binder/bound_statement.h"
#include "binder/expression/existential_subquery_expression.h"
#include "join_order_enumerator.h"
#include "projection_planner.h"
#include "update_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class QueryPlanner {
    friend class JoinOrderEnumerator;
    friend class ProjectionPlanner;
    friend class UpdatePlanner;
    friend class ASPOptimizer;

public:
    explicit QueryPlanner(const Catalog& catalog,
        const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics)
        : catalog{catalog}, joinOrderEnumerator{catalog, nodesStatistics, relsStatistics, this},
          projectionPlanner{this}, updatePlanner{} {}

    vector<unique_ptr<LogicalPlan>> getAllPlans(const BoundStatement& boundStatement);

    unique_ptr<LogicalPlan> getShortestPathPlan(const BoundStatement& boundStatement);

    inline unique_ptr<LogicalPlan> getBestPlan(const BoundStatement& boundStatement) {
        return getBestPlan(getAllPlans(boundStatement));
    }

private:
    unique_ptr<LogicalPlan> getBestPlan(vector<unique_ptr<LogicalPlan>> plans);

    vector<unique_ptr<LogicalPlan>> planSingleQuery(const NormalizedSingleQuery& singleQuery);
    vector<unique_ptr<LogicalPlan>> planQueryPart(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);

    // Reading clause planning
    void planReadingClause(
        BoundReadingClause* boundReadingClause, vector<unique_ptr<LogicalPlan>>& prevPlans);
    void planMatchClause(
        BoundReadingClause* boundReadingClause, vector<unique_ptr<LogicalPlan>>& plans);
    void planUnwindClause(
        BoundReadingClause* boundReadingClause, vector<unique_ptr<LogicalPlan>>& plans);

    // CTE & subquery planning
    void planOptionalMatch(const QueryGraphCollection& queryGraphCollection,
        expression_vector& predicates, LogicalPlan& outerPlan);
    void planRegularMatch(const QueryGraphCollection& queryGraphCollection,
        expression_vector& predicates, LogicalPlan& prevPlan);
    void planExistsSubquery(shared_ptr<Expression>& subquery, LogicalPlan& outerPlan);
    void planSubqueryIfNecessary(const shared_ptr<Expression>& expression, LogicalPlan& plan);

    static void appendAccumulate(LogicalPlan& plan);

    static void appendExpressionsScan(const expression_vector& expressions, LogicalPlan& plan);

    static void appendDistinct(const expression_vector& expressionsToDistinct, LogicalPlan& plan);

    static void appendUnwind(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan);

    static void appendFlattens(const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);
    // return position of the only unFlat group
    // or position of any flat group if there is no unFlat group.
    static uint32_t appendFlattensButOne(
        const unordered_set<uint32_t>& groupsPos, LogicalPlan& plan);
    static void appendFlattenIfNecessary(
        const shared_ptr<Expression>& expression, LogicalPlan& plan);
    static inline void appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan) {
        auto expressions = plan.getSchema()->getExpressionsInScope(groupPos);
        assert(!expressions.empty());
        appendFlattenIfNecessary(expressions[0], plan);
    }

    void appendFilter(const shared_ptr<Expression>& expression, LogicalPlan& plan);

    void appendScanNodePropIfNecessary(const expression_vector& propertyExpressions,
        shared_ptr<NodeExpression> node, LogicalPlan& plan);

    unique_ptr<LogicalPlan> createUnionPlan(
        vector<unique_ptr<LogicalPlan>>& childrenPlans, bool isUnionAll);

    static vector<unique_ptr<LogicalPlan>> getInitialEmptyPlans();

    expression_vector getPropertiesForNode(NodeExpression& node);
    expression_vector getPropertiesForRel(RelExpression& rel);

    static vector<vector<unique_ptr<LogicalPlan>>> cartesianProductChildrenPlans(
        vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans);

private:
    const Catalog& catalog;
    expression_vector propertiesToScan;
    JoinOrderEnumerator joinOrderEnumerator;
    ProjectionPlanner projectionPlanner;
    UpdatePlanner updatePlanner;
};

} // namespace planner
} // namespace kuzu
