#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/binder/include/bound_statements/bound_load_csv_statement.h"
#include "src/binder/include/expression/existential_subquery_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/planner/include/enumerator_context.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/storage/include/graph.h"

using namespace graphflow::binder;
using namespace graphflow::storage;

namespace graphflow {
namespace planner {

class Enumerator {

public:
    explicit Enumerator(const Graph& graph)
        : graph{graph}, context{make_unique<EnumeratorContext>()} {}

    unique_ptr<LogicalPlan> getBestPlan(const BoundSingleQuery& singleQuery);

    vector<unique_ptr<LogicalPlan>> enumeratePlans(const BoundSingleQuery& singleQuery);

private:
    void enumerateQueryPart(BoundQueryPart& queryPart);
    void enumerateProjectionBody(
        const BoundProjectionBody& projectionBody, bool isRewritingAllProperties);
    void enumerateReadingStatement(BoundReadingStatement& readingStatement);
    void enumerateLoadCSVStatement(const BoundLoadCSVStatement& loadCSVStatement);
    void enumerateSubplans(const vector<shared_ptr<Expression>>& whereExpressions);
    void enumerateInitialScan(const vector<shared_ptr<Expression>>& whereExpressions);
    void enumerateNextLevel(const vector<shared_ptr<Expression>>& whereExpressions);
    void enumerateHashJoin(const vector<shared_ptr<Expression>>& whereExpressions);
    void enumerateSingleRel(const vector<shared_ptr<Expression>>& whereExpressions);

    // Flatten all. Return any flat groupPos.
    uint32_t planSubquery(
        ExistentialSubqueryExpression& subqueryExpression, LogicalPlan& outerPlan);
    unique_ptr<EnumeratorContext> enterSubquery();
    void exitSubquery(unique_ptr<EnumeratorContext> prevContext);

    void appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan);
    void appendScan(const NodeExpression& queryNode, LogicalPlan& plan);
    void appendSelectScan(LogicalPlan& plan);
    void appendExtendAndNecessaryFilters(const RelExpression& queryRel, Direction direction,
        const vector<shared_ptr<Expression>>& expressionsToFilter, LogicalPlan& plan);
    void appendExtend(const RelExpression& queryRel, Direction direction, LogicalPlan& plan);
    // Flatten all but one. Return the unFlat groupPos
    uint32_t appendNecessaryFlattens(
        const unordered_set<uint32_t>& unFlatGroupsPos, LogicalPlan& plan);
    void appendFlatten(uint32_t groupPos, LogicalPlan& plan);
    void appendLogicalHashJoin(
        const NodeExpression& joinNode, LogicalPlan& buildPlan, LogicalPlan& probePlan);
    void appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan);
    bool appendIntersect(const string& leftNodeID, const string& rightNodeID, LogicalPlan& plan);
    void appendProjection(const vector<shared_ptr<Expression>>& expressions, LogicalPlan& plan,
        bool isRewritingAllProperties);
    // Return groupPos to select/write
    uint32_t appendNecessaryOperatorForExpression(Expression& expression, LogicalPlan& plan);
    void appendNecessaryScans(Expression& expression, LogicalPlan& plan);
    void appendScanNodeProperty(const PropertyExpression& propertyExpression, LogicalPlan& plan);
    void appendScanRelProperty(const PropertyExpression& propertyExpression, LogicalPlan& plan);
    void appendMultiplicityReducer(LogicalPlan& plan);
    void appendLimit(uint64_t limitNumber, LogicalPlan& plan);
    void appendSkip(uint64_t skipNumber, LogicalPlan& plan);

private:
    const Graph& graph;
    unique_ptr<EnumeratorContext> context;
};

} // namespace planner
} // namespace graphflow
