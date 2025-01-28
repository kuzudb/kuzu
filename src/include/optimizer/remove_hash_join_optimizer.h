#pragma once

#include "logical_operator_visitor.h"
#include "planner/operator/logical_plan.h"
#include "common/types/internal_id_t.h"

namespace kuzu {
namespace optimizer {

class RemoveHashBuildValidator {
public:
    explicit RemoveHashBuildValidator(common::table_id_t tableId)
        : tableId(tableId) {}

    bool validate(planner::LogicalOperator* op);

private:
    bool validateScanNodeTable(planner::LogicalOperator* op) const;

private:
    common::table_id_t tableId;
};

class NodeTableCollector {
public:
    bool validateAndCollect(planner::LogicalOperator* op);

private:
    bool visitScanNodeTable(planner::LogicalOperator* op);

public:
    common::table_id_t tableId = common::INVALID_TABLE_ID;
};

class ReplaceNodeTableOnProbeSide : public LogicalOperatorVisitor {
public:
    explicit ReplaceNodeTableOnProbeSide(common::table_id_t tableId,
                                         std::shared_ptr<planner::LogicalOperator> rpl)
            : tableId(tableId), replacement(rpl) {};

    std::shared_ptr<planner::LogicalOperator> rewrite(
            const std::shared_ptr<planner::LogicalOperator>& op);

private:
    std::shared_ptr<planner::LogicalOperator> visitScanNodeTableReplace(
            std::shared_ptr<planner::LogicalOperator> op);

private:
    common::table_id_t tableId;
    std::shared_ptr<planner::LogicalOperator> replacement;
};

// Remove hash join optimizer
class RemoveHashJoinOptimizer : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);

    std::shared_ptr<planner::LogicalOperator> visitOperator(
            const std::shared_ptr<planner::LogicalOperator>& op);

private:
    std::shared_ptr<planner::LogicalOperator> visitHashJoinReplace(
            std::shared_ptr<planner::LogicalOperator> op);
};

} // namespace optimizer
} // namespace kuzu