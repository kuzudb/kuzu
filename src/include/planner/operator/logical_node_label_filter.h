#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalNodeLabelFilter : public LogicalOperator {
public:
    LogicalNodeLabelFilter(std::shared_ptr<binder::Expression> nodeID,
        std::unordered_set<common::table_id_t> tableIDSet, std::shared_ptr<LogicalOperator> child,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator{LogicalOperatorType::NODE_LABEL_FILTER, std::move(child),
              std::move(printInfo)},
          nodeID{std::move(nodeID)}, tableIDSet{std::move(tableIDSet)} {}

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const final { return nodeID->toString(); }

    inline std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
    inline std::unordered_set<common::table_id_t> getTableIDSet() const { return tableIDSet; }

    std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalNodeLabelFilter>(nodeID, tableIDSet, children[0]->copy(),
            printInfo->copy());
    }

private:
    std::shared_ptr<binder::Expression> nodeID;
    std::unordered_set<common::table_id_t> tableIDSet;
};

} // namespace planner
} // namespace kuzu
