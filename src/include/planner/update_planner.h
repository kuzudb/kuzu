#pragma once

#include "binder/query/updating_clause/bound_create_clause.h"
#include "binder/query/updating_clause/bound_delete_clause.h"
#include "binder/query/updating_clause/bound_set_clause.h"
#include "binder/query/updating_clause/bound_updating_clause.h"
#include "catalog/catalog.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace planner {

class QueryPlanner;

class UpdatePlanner {
public:
    UpdatePlanner(QueryPlanner* queryPlanner) : queryPlanner{queryPlanner} {};

    inline void planUpdatingClause(binder::BoundUpdatingClause& updatingClause,
        std::vector<std::unique_ptr<LogicalPlan>>& plans) {
        for (auto& plan : plans) {
            planUpdatingClause(updatingClause, *plan);
        }
    }

private:
    void planUpdatingClause(binder::BoundUpdatingClause& updatingClause, LogicalPlan& plan);

    void planCreate(binder::BoundCreateClause& createClause, LogicalPlan& plan);
    void appendCreateNode(const std::vector<std::unique_ptr<binder::BoundCreateNode>>& createNodes,
        LogicalPlan& plan);
    void appendCreateRel(
        const std::vector<std::unique_ptr<binder::BoundCreateRel>>& createRels, LogicalPlan& plan);

    void planSet(binder::BoundSetClause& setClause, LogicalPlan& plan);
    void appendSetNodeProperty(
        const std::vector<std::unique_ptr<binder::BoundSetNodeProperty>>& setNodeProperties,
        LogicalPlan& plan);
    void appendSetRelProperty(
        const std::vector<std::unique_ptr<binder::BoundSetRelProperty>>& setRelProperties,
        LogicalPlan& plan);

    void planDelete(binder::BoundDeleteClause& deleteClause, LogicalPlan& plan);
    void appendDeleteNode(const std::vector<std::unique_ptr<binder::BoundDeleteNode>>& deleteNodes,
        LogicalPlan& plan);
    void appendDeleteRel(
        const std::vector<std::shared_ptr<binder::RelExpression>>& deleteRels, LogicalPlan& plan);

private:
    QueryPlanner* queryPlanner;
};

} // namespace planner
} // namespace kuzu
