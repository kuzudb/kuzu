#pragma once

#include <unordered_map>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

// This optimization extract all property scanners and append them on top of scan node ID / extend.
class PropertyScanPushDown {

public:
    shared_ptr<LogicalOperator> rewrite(shared_ptr<LogicalOperator> op);

private:
    shared_ptr<LogicalOperator> rewriteScanNodeID(const shared_ptr<LogicalOperator>& op);

    shared_ptr<LogicalOperator> rewriteExtend(const shared_ptr<LogicalOperator>& op);

    shared_ptr<LogicalOperator> rewriteLeftNestedLoopJoin(const shared_ptr<LogicalOperator>& op);

    shared_ptr<LogicalOperator> rewriteScanNodeProperty(const shared_ptr<LogicalOperator>& op);

    shared_ptr<LogicalOperator> rewriteScanRelProperty(const shared_ptr<LogicalOperator>& op);

    shared_ptr<LogicalOperator> applyPropertyScansIfNecessary(
        const string& nodeID, const shared_ptr<LogicalOperator>& op);

    void rewriteChildrenOperators(const shared_ptr<LogicalOperator>& op);

    void addPropertyScan(const string& nodeID, const shared_ptr<LogicalOperator>& op);

private:
    unordered_map<string, vector<shared_ptr<LogicalOperator>>> nodeIDToPropertyScansMap;
};

} // namespace planner
} // namespace graphflow
