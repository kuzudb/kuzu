#pragma once

#include "src/binder/query/updating_clause/include/bound_create_clause.h"
#include "src/binder/query/updating_clause/include/bound_delete_clause.h"
#include "src/binder/query/updating_clause/include/bound_set_clause.h"
#include "src/binder/query/updating_clause/include/bound_updating_clause.h"
#include "src/catalog/include/catalog.h"
#include "src/planner/logical_plan/include/logical_plan.h"

namespace graphflow {
namespace planner {

class Enumerator;

class UpdatePlanner {
public:
    UpdatePlanner(const catalog::Catalog& catalog, Enumerator* enumerator)
        : catalog{catalog}, enumerator{enumerator} {};

    inline void planUpdatingClause(
        BoundUpdatingClause& updatingClause, vector<unique_ptr<LogicalPlan>>& plans) {
        for (auto& plan : plans) {
            planUpdatingClause(updatingClause, *plan);
        }
    }

private:
    void planUpdatingClause(BoundUpdatingClause& updatingClause, LogicalPlan& plan);
    void planPropertyUpdateInfo(
        shared_ptr<Expression> property, shared_ptr<Expression> target, LogicalPlan& plan);

    void appendTableScan(BoundCreateClause& createClause, LogicalPlan& plan);
    void appendCreate(BoundCreateClause& createClause, LogicalPlan& plan);
    void appendSet(BoundSetClause& setClause, LogicalPlan& plan);
    void appendDelete(BoundDeleteClause& deleteClause, LogicalPlan& plan);

private:
    const catalog::Catalog& catalog;
    Enumerator* enumerator;
};

} // namespace planner
} // namespace graphflow
