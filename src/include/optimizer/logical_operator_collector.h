#pragma once

#include "logical_operator_visitor.h"

namespace kuzu {
namespace optimizer {

class LogicalOperatorCollector : public LogicalOperatorVisitor {
public:
    ~LogicalOperatorCollector() = default;

    void collect(planner::LogicalOperator* op);

    inline bool hasOperators() const { return !ops.empty(); }
    inline std::vector<planner::LogicalOperator*> getOperators() const { return ops; }

protected:
    std::vector<planner::LogicalOperator*> ops;
};

class LogicalFlattenCollector : public LogicalOperatorCollector {
protected:
    void visitFlatten(planner::LogicalOperator* op) final { ops.push_back(op); }
};

class LogicalFilterCollector : public LogicalOperatorCollector {
protected:
    void visitFilter(planner::LogicalOperator* op) final { ops.push_back(op); }
};

class LogicalScanInternalIDCollector : public LogicalOperatorCollector {
protected:
    void visitScanInternalID(planner::LogicalOperator* op) final { ops.push_back(op); }
};

class LogicalIndexScanNodeCollector : public LogicalOperatorCollector {
protected:
    void visitIndexScanNode(planner::LogicalOperator* op) final { ops.push_back(op); }
};

class LogicalRecursiveExtendCollector : public LogicalOperatorCollector {
protected:
    void visitRecursiveExtend(planner::LogicalOperator* op) final { ops.push_back(op); }
};

} // namespace optimizer
} // namespace kuzu
