#pragma once

#include "src/planner/include/logical_plan/logical_plan.h"
#include "src/storage/include/catalog.h"

using namespace graphflow::storage;

namespace graphflow {
namespace planner {

class QueryPlanner {

public:
    static vector<unique_ptr<LogicalPlan>> enumeratePlans(string query, const Catalog& catalog);
};

} // namespace planner
} // namespace graphflow
