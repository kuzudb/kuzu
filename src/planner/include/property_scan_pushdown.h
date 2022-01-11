#pragma once

#include <unordered_map>

#include "src/planner/include/logical_plan/logical_plan.h"

namespace graphflow {
namespace planner {

// This optimization extract all property scanners and append them on top of scan node ID / extend.
class PropertyScanPushDown {

public:
    inline void rewrite(LogicalPlan& logicalPlan) {
        logicalPlan.lastOperator = rewrite(logicalPlan.lastOperator, *logicalPlan.schema);
    }

private:
    shared_ptr<LogicalOperator> rewrite(shared_ptr<LogicalOperator> op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteScanNodeID(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteExtend(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteScanNodeProperty(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteScanRelProperty(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteAggregate(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteOrderBy(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteHashJoin(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteExists(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteLeftNestedLoopJoin(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> applyPropertyScansIfNecessary(
        const string& nodeID, const shared_ptr<LogicalOperator>& op, Schema& schema);

    void rewriteChildrenOperators(const shared_ptr<LogicalOperator>& op, Schema& schema);

    void addPropertyScan(const string& nodeID, const shared_ptr<LogicalOperator>& op);

    // For operators that compute a new schema from an older schema, i.e. hashJoin,
    // leftNestedLoopJoin and orderBy, any property scanners, that are pushed down under these
    // operators, were removed from new schema and being added to older schema. We also need to
    // append them back to the new schema if the operator is merging vectors from old schema into
    // new schema.
    void addPropertyScansToSchema(Schema& schema);

    unordered_set<string> getRemainingPropertyExpressionNames();

private:
    unordered_map<string, vector<shared_ptr<LogicalOperator>>> nodeIDToPropertyScansMap;
};

} // namespace planner
} // namespace graphflow
