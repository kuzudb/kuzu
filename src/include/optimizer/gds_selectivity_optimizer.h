#pragma once

#include "logical_operator_visitor.h"
#include "planner/operator/logical_plan.h"
#include "binder/expression/node_expression.h"

using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace optimizer {

class FindUsefulProperty: public LogicalOperatorVisitor {
public:
    void collect(planner::LogicalOperator* op);

private:
    void visitFilter(planner::LogicalOperator* op) override;

public:
    expression_set used_properties;
};

struct LookupProperty {
    std::vector<common::table_id_t> tableIDs;
    std::shared_ptr<Expression> idProperty;
    expression_vector properties;
};

class RemoveAccumulate: public LogicalOperatorVisitor {
public:
    explicit RemoveAccumulate(bool onlyAccumulate = false)
        : onlyAccumulate(onlyAccumulate) {}

    std::shared_ptr<planner::LogicalOperator> rewrite(
        const std::shared_ptr<planner::LogicalOperator>& op);

private:
    std::shared_ptr<planner::LogicalOperator>
    visitAccumulateReplace(std::shared_ptr<planner::LogicalOperator> op) override;

    std::shared_ptr<planner::LogicalOperator>
    visitSemiMaskerReplace(std::shared_ptr<planner::LogicalOperator> op) override;

    std::shared_ptr<planner::LogicalOperator>
    visitFlattenReplace(std::shared_ptr<planner::LogicalOperator> op) override;

private:
    bool onlyAccumulate;
};

class RemoveUnusedNodes: public LogicalOperatorVisitor {
public:
    explicit RemoveUnusedNodes(const expression_set& used_properties)
        : used_properties(used_properties), lookup_properties(std::vector<LookupProperty>()) {}

    std::shared_ptr<planner::LogicalOperator> rewrite(
        const std::shared_ptr<planner::LogicalOperator>& op);

private:
    std::shared_ptr<planner::LogicalOperator> visitHashJoinReplace(
            std::shared_ptr<planner::LogicalOperator> op) override;

    std::shared_ptr<planner::LogicalOperator>
    visitScanNodeTableReplace(std::shared_ptr<planner::LogicalOperator> op) override;

    std::shared_ptr<planner::LogicalOperator>
    visitProjectionReplace(std::shared_ptr<planner::LogicalOperator> op) override;

    bool isBuildSideNeeded(const std::shared_ptr<planner::LogicalOperator>& op) const;

    bool isProbeSideNeeded(const std::shared_ptr<planner::LogicalOperator>& op) const;

public:
    expression_set used_properties;
    std::vector<LookupProperty> lookup_properties;
};

// Remove hash join
class GDSSelectivityOptimizer : public LogicalOperatorVisitor {
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
