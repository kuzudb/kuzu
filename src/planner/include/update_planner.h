#pragma once

#include "src/binder/query/set_clause/include/bound_set_clause.h"
#include "src/planner/logical_plan/include/logical_plan.h"

namespace graphflow {
namespace planner {

class Enumerator;

class UpdatePlanner {

public:
    static void planSetClause(BoundSetClause& setClause, vector<unique_ptr<LogicalPlan>>& plans);

private:
    static void appendSink(LogicalPlan& plan);
    static void appendSet(BoundSetClause& setClause, LogicalPlan& plan);
};

} // namespace planner
} // namespace graphflow
