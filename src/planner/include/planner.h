#pragma once

#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

class Planner {

public:
    static unique_ptr<LogicalPlan> getBestPlan(
        const Catalog& catalog, const NodesMetadata& nodesMetadata, const BoundStatement& query);

    static vector<unique_ptr<LogicalPlan>> getAllPlans(
        const Catalog& catalog, const NodesMetadata& nodesMetadata, const BoundStatement& query);

private:
    static unique_ptr<LogicalPlan> optimize(unique_ptr<LogicalPlan> plan);
};

} // namespace planner
} // namespace graphflow
