#pragma once

#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

class Planner {
public:
    static inline unique_ptr<LogicalPlan> getThreeHopPlan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getThreeHopPlan(statement);
    }
    static inline unique_ptr<LogicalPlan> getTrianglePlan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getTrianglePlan(statement);
    }
    static inline unique_ptr<LogicalPlan> getCyclePlan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getCyclePlan(statement);
    }
    static inline unique_ptr<LogicalPlan> getCliquePlan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getCliquePlan(statement);
    }
    static inline unique_ptr<LogicalPlan> getIS01Plan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getIS1Plan(statement);
    }
    static inline unique_ptr<LogicalPlan> getIS02Plan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getIS2Plan(statement);
    }
    static inline unique_ptr<LogicalPlan> getIS03Plan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getIS3Plan(statement);
    }
    static inline unique_ptr<LogicalPlan> getIS04Plan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getIS4Plan(statement);
    }
    static inline unique_ptr<LogicalPlan> getIS05Plan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getIS5Plan(statement);
    }
    static inline unique_ptr<LogicalPlan> getIS06Plan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getIS6Plan(statement);
    }
    static inline unique_ptr<LogicalPlan> getIS07Plan(const Catalog& catalog,
        const NodesMetadata& nodesMetadata, const BoundStatement& statement) {
        return Enumerator(catalog, nodesMetadata).getIS7Plan(statement);
    }

    static unique_ptr<LogicalPlan> getBestPlan(
        const Catalog& catalog, const NodesMetadata& nodesMetadata, const BoundStatement& query);

    static vector<unique_ptr<LogicalPlan>> getAllPlans(
        const Catalog& catalog, const NodesMetadata& nodesMetadata, const BoundStatement& query);

private:
    static unique_ptr<LogicalPlan> optimize(unique_ptr<LogicalPlan> plan);
};

} // namespace planner
} // namespace graphflow
