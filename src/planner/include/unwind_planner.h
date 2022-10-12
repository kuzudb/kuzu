#pragma once

#include "src/binder/query/reading_clause/include/bound_unwind_clause.h"
#include "src/catalog/include/catalog.h"
#include "src/planner/logical_plan/include/logical_plan.h"

namespace graphflow {
namespace planner {

class Enumerator;

class UnwindPlanner {
public:
    UnwindPlanner(const catalog::Catalog& catalog, Enumerator* enumerator)
        : catalog{catalog}, enumerator{enumerator} {};

    inline void planUnwindClause(
        BoundUnwindClause& unwindClause, vector<unique_ptr<LogicalPlan>>& plans) {
        for (auto& plan : plans) {
            planUnwindClause(unwindClause, *plan);
        }
    }

private:
    void planUnwindClause(BoundUnwindClause& unwindClause, LogicalPlan& plan);
    void appendUnwindClause(BoundUnwindClause& unwindClause, LogicalPlan& plan);

private:
    const catalog::Catalog& catalog;
    Enumerator* enumerator;
};
} // namespace planner
} // namespace graphflow
