#pragma once

#include "src/binder/query/updating_clause/include/bound_create_clause.h"
#include "src/binder/query/updating_clause/include/bound_delete_clause.h"
#include "src/binder/query/updating_clause/include/bound_set_clause.h"
#include "src/binder/query/updating_clause/include/bound_updating_clause.h"
#include "src/catalog/include/catalog.h"
#include "src/planner/logical_plan/include/logical_plan.h"

namespace kuzu {
namespace planner {

using namespace kuzu::catalog;

class QueryPlanner;

class UpdatePlanner {
public:
    UpdatePlanner(const catalog::Catalog& catalog, QueryPlanner* queryPlanner)
        : catalog{catalog}, queryPlanner{queryPlanner} {};

    inline void planUpdatingClause(
        BoundUpdatingClause& updatingClause, vector<unique_ptr<LogicalPlan>>& plans) {
        for (auto& plan : plans) {
            planUpdatingClause(updatingClause, *plan);
        }
    }

private:
    void planUpdatingClause(BoundUpdatingClause& updatingClause, LogicalPlan& plan);
    void planSetItem(expression_pair setItem, LogicalPlan& plan);

    void planCreate(BoundCreateClause& createClause, LogicalPlan& plan);
    void appendCreateNode(BoundCreateClause& createClause, LogicalPlan& plan);
    void appendCreateRel(BoundCreateClause& createClause, LogicalPlan& plan);

    inline void appendSet(BoundSetClause& setClause, LogicalPlan& plan) {
        appendSet(setClause.getSetItems(), plan);
    }
    void appendSet(vector<expression_pair> setItems, LogicalPlan& plan);
    void appendDelete(BoundDeleteClause& deleteClause, LogicalPlan& plan);

    vector<expression_pair> splitSetItems(vector<expression_pair> setItems, bool isStructured);

private:
    const Catalog& catalog;
    QueryPlanner* queryPlanner;
};

} // namespace planner
} // namespace kuzu
