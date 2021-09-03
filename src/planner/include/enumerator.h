#pragma once

#include "src/binder/include/expression/property_expression.h"
#include "src/planner/include/join_order_enumerator.h"
#include "src/planner/include/projection_enumerator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class Enumerator {
    friend class JoinOrderEnumerator;
    friend class ProjectionEnumerator;

public:
    explicit Enumerator(const Graph& graph)
        : joinOrderEnumerator{graph}, projectionEnumerator{graph.getCatalog()} {}

    unique_ptr<LogicalPlan> getBestPlan(const BoundSingleQuery& singleQuery);
    vector<unique_ptr<LogicalPlan>> enumeratePlans(const BoundSingleQuery& singleQuery);

private:
    vector<unique_ptr<LogicalPlan>> enumerateQueryPart(
        const NormalizedQueryPart& queryPart, vector<unique_ptr<LogicalPlan>> prevPlans);

    void appendLoadCSV(const BoundLoadCSVStatement& loadCSVStatement, LogicalPlan& plan);
    static uint32_t appendFlattensIfNecessary(
        const unordered_set<uint32_t>& unFlatGroupsPos, LogicalPlan& plan);
    static void appendFlattenIfNecessary(uint32_t groupPos, LogicalPlan& plan);
    static void appendFilter(shared_ptr<Expression> expression, LogicalPlan& plan);
    static void appendScanPropertiesIfNecessary(Expression& expression, LogicalPlan& plan);
    static void appendScanNodeProperty(
        const PropertyExpression& propertyExpression, LogicalPlan& plan);
    static void appendScanRelProperty(
        const PropertyExpression& propertyExpression, LogicalPlan& plan);

    static unordered_set<uint32_t> getUnFlatGroupsPos(Expression& expression, const Schema& schema);
    static uint32_t getAnyGroupPos(Expression& expression, const Schema& schema);

private:
    JoinOrderEnumerator joinOrderEnumerator;
    ProjectionEnumerator projectionEnumerator;
};

} // namespace planner
} // namespace graphflow
