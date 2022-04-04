#pragma once

#include "src/binder/query/include/normalized_single_query.h"
#include "src/catalog/include/catalog.h"
#include "src/planner/include/join_order_enumerator_context.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace planner {

class Enumerator;

const double PREDICATE_SELECTIVITY = 0.2;

/**
 * JoinOrderEnumerator is currently responsible for
 *      join order enumeration
 *      filter push down
 */
class JoinOrderEnumerator {
    friend class Enumerator;

public:
    JoinOrderEnumerator(const Catalog& catalog, Enumerator* enumerator)
        : catalog{catalog},
          enumerator{enumerator}, context{make_unique<JoinOrderEnumeratorContext>()} {};

    vector<unique_ptr<LogicalPlan>> enumerateJoinOrder(const QueryGraph& queryGraph,
        const shared_ptr<Expression>& queryGraphPredicate,
        vector<unique_ptr<LogicalPlan>> prevPlans);

    inline void resetState() { context->resetState(); }

private:
    unique_ptr<JoinOrderEnumeratorContext> enterSubquery(expression_vector expressionsToScan);
    void exitSubquery(unique_ptr<JoinOrderEnumeratorContext> prevContext);

    // join order enumeration functions
    void enumerateResultScan();
    void enumerateSingleNode();
    void enumerateHashJoin();
    void enumerateSingleRel();

    // append logical operator functions
    void appendResultScan(const expression_vector& expressionsToSelect, LogicalPlan& plan);
    void appendScanNodeID(NodeExpression& queryNode, LogicalPlan& plan);
    void appendExtendFiltersAndScanProperties(const RelExpression& queryRel, RelDirection direction,
        const expression_vector& expressionsToFilter, LogicalPlan& plan);
    void appendExtend(const RelExpression& queryRel, RelDirection direction, LogicalPlan& plan);
    void appendLogicalHashJoin(
        const NodeExpression& joinNode, LogicalPlan& probePlan, LogicalPlan& buildPlan);
    // appendIntersect return false if a nodeID is flat in which case we should use filter
    bool appendIntersect(const string& leftNodeID, const string& rightNodeID, LogicalPlan& plan);

    // helper functions
    uint64_t getExtensionRate(label_t boundNodeLabel, label_t relLabel, RelDirection relDirection);

private:
    const catalog::Catalog& catalog;
    Enumerator* enumerator;
    unique_ptr<JoinOrderEnumeratorContext> context;
};

} // namespace planner
} // namespace graphflow
