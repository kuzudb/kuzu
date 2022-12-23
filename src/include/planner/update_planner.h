#pragma once

#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "binder/query/updating_clause/bound_updating_clause.h"
#include "catalog/catalog.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace planner {

class UpdatePlanner {
public:
    UpdatePlanner() = default;

    inline void planUpdatingClause(
        BoundUpdatingClause& updatingClause, vector<unique_ptr<LogicalPlan>>& plans) {
        for (auto& plan : plans) {
            planUpdatingClause(updatingClause, *plan);
        }
    }

private:
    void planUpdatingClause(BoundUpdatingClause& updatingClause, LogicalPlan& plan);

    void planCreate(BoundCreateClause& createClause, LogicalPlan& plan);
    void appendCreateNode(
        const vector<unique_ptr<BoundCreateNode>>& createNodes, LogicalPlan& plan);
    void appendCreateRel(const vector<unique_ptr<BoundCreateRel>>& createRels, LogicalPlan& plan);

    void planSet(BoundSetClause& setClause, LogicalPlan& plan);
    void appendSetNodeProperty(
        const vector<unique_ptr<BoundSetNodeProperty>>& setNodeProperties, LogicalPlan& plan);
    void appendSetRelProperty(
        const vector<unique_ptr<BoundSetRelProperty>>& setRelProperties, LogicalPlan& plan);

    void planDelete(BoundDeleteClause& deleteClause, LogicalPlan& plan);
    void appendDeleteNode(
        const vector<unique_ptr<BoundDeleteNode>>& deleteNodes, LogicalPlan& plan);
    void appendDeleteRel(const vector<shared_ptr<RelExpression>>& deleteRels, LogicalPlan& plan);

    void flattenRel(const RelExpression& rel, LogicalPlan& plan);
};

} // namespace planner
} // namespace kuzu
