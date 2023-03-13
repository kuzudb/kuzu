#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"
#include "logical_scan_node.h"

namespace kuzu {
namespace planner {

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(std::shared_ptr<binder::Expression> nodeID,
        std::vector<LogicalOperator*> scanNodes, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SEMI_MASKER, std::move(child)},
          nodeID{std::move(nodeID)}, scanNodes{std::move(scanNodes)} {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override { return nodeID->toString(); }

    inline std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
    inline bool isMultiLabel() const {
        return ((LogicalScanNode*)scanNodes[0])->getNode()->isMultiLabeled();
    }
    inline std::vector<LogicalOperator*> getScanNodes() const { return scanNodes; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSemiMasker>(nodeID, scanNodes, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> nodeID;
    std::vector<LogicalOperator*> scanNodes;
};

} // namespace planner
} // namespace kuzu
