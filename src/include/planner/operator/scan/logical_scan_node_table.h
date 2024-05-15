#pragma once

#include "binder/expression/expression_util.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalScanNodeTable final : public LogicalOperator {
public:
    LogicalScanNodeTable(std::shared_ptr<binder::Expression> nodeID,
        std::vector<common::table_id_t> nodeTableIDs, binder::expression_vector properties)
        : LogicalOperator{LogicalOperatorType::SCAN_NODE_TABLE}, nodeID{std::move(nodeID)},
          nodeTableIDs{std::move(nodeTableIDs)}, properties{std::move(properties)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(properties);
    }

    std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
    std::vector<common::table_id_t> getTableIDs() const { return nodeTableIDs; }
    binder::expression_vector getProperties() const { return properties; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeTable>(nodeID, nodeTableIDs, properties);
    }

private:
    std::shared_ptr<binder::Expression> nodeID;
    std::vector<common::table_id_t> nodeTableIDs;
    binder::expression_vector properties;
};

} // namespace planner
} // namespace kuzu
