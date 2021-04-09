#pragma once

#include "src/common/include/configs.h"
#include "src/planner/include/binder.h"
#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/filter/logical_filter.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/property_reader/logical_node_property_reader.h"
#include "src/planner/include/logical_plan/operator/property_reader/logical_rel_property_reader.h"
#include "src/planner/include/logical_plan/operator/scan/logical_scan.h"
#include "src/planner/include/subgraph_plan_table.h"

namespace graphflow {
namespace planner {

class Enumerator {

public:
    explicit Enumerator(const BoundSingleQuery& boundSingleQuery);

    vector<unique_ptr<LogicalPlan>> enumeratePlans();

private:
    void enumerateSingleQueryNode();

    void enumerateNextNumQueryRel(uint32_t& numEnumeratedQueryRels);

    void enumerateHashJoin(uint32_t nextNumEnumeratedQueryRels);

    void enumerateExtend(uint32_t nextNumEnumeratedQueryRels);

    void appendFiltersIfPossible(const SubqueryGraph& leftPrevSubgraph,
        const SubqueryGraph& rightPrevSubgraph, const SubqueryGraph& subgraph, LogicalPlan& plan);

    void appendFiltersIfPossible(
        const SubqueryGraph& prevSubgraph, const SubqueryGraph& subgraph, LogicalPlan& plan);

    void appendFilterAndNecessaryScans(shared_ptr<LogicalExpression> expr, LogicalPlan& plan);

    void appendPropertyReader(const string& varAndPropertyName, LogicalPlan& plan);

    void appendNodePropertyReader(
        const string& nodeName, const string& propertyName, LogicalPlan& plan);

    void appendRelPropertyReader(
        const string& relName, const string& propertyName, LogicalPlan& plan);

private:
    unique_ptr<SubgraphPlanTable> subgraphPlanTable; // cached subgraph plans
    const QueryGraph& queryGraph;
    vector<pair<shared_ptr<LogicalExpression>, unordered_set<string>>>
        expressionsAndIncludedVariables;
};

} // namespace planner
} // namespace graphflow
