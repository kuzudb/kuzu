#pragma once

#include "src/binder/include/expression/property_expression.h"
#include "src/planner/include/join_order_enumerator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class Enumerator {
    friend class JoinOrderEnumerator;

public:
    explicit Enumerator(const Graph& graph) : graph{graph}, joinOrderEnumerator{graph} {}

    unique_ptr<LogicalPlan> getBestPlan(const BoundSingleQuery& singleQuery);
    vector<unique_ptr<LogicalPlan>> enumeratePlans(const BoundSingleQuery& singleQuery);

private:
    vector<unique_ptr<LogicalPlan>> enumerateQueryPart(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);
    void enumerateProjectionBody(const BoundProjectionBody& projectionBody,
        const vector<unique_ptr<LogicalPlan>>& plans, bool isFinalReturn);

    void appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan);
    static uint32_t appendFlattensIfNecessary(
        const unordered_set<uint32_t>& unFlatGroupsPos, LogicalPlan& plan);
    static void appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan);
    static void appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan);
    void appendProjection(const vector<shared_ptr<Expression>>& expressions, LogicalPlan& plan,
        bool isRewritingAllProperties);
    static void appendScanPropertiesIfNecessary(Expression& expression, LogicalPlan& plan);
    static void appendScanNodeProperty(
        const PropertyExpression& propertyExpression, LogicalPlan& plan);
    static void appendScanRelProperty(
        const PropertyExpression& propertyExpression, LogicalPlan& plan);
    void appendMultiplicityReducer(LogicalPlan& plan);
    void appendLimit(uint64_t limitNumber, LogicalPlan& plan);
    void appendSkip(uint64_t skipNumber, LogicalPlan& plan);

private:
    const Graph& graph;
    JoinOrderEnumerator joinOrderEnumerator;
};

} // namespace planner
} // namespace graphflow
