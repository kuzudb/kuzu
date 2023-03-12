#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "logical_scan_node.h"

namespace kuzu {
namespace planner {

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(LogicalScanNode* scanNode, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SEMI_MASKER, std::move(child)}, scanNode{scanNode} {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override {
        return scanNode->getNode()->toString();
    }

    inline LogicalScanNode* getScanNode() const { return scanNode; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSemiMasker>(scanNode, children[0]->copy());
    }

private:
    LogicalScanNode* scanNode;
};

} // namespace planner
} // namespace kuzu
