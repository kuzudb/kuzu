#pragma once

#include "binder/expression/expression_util.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalScanNodeProperty : public LogicalOperator {
public:
    LogicalScanNodeProperty(std::shared_ptr<binder::Expression> nodeID,
        std::vector<common::table_id_t> nodeTableIDs, binder::expression_vector properties,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SCAN_NODE_PROPERTY, std::move(child)},
          nodeID{std::move(nodeID)}, nodeTableIDs{std::move(nodeTableIDs)}, properties{std::move(
                                                                                properties)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(properties);
    }

    inline std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
    inline std::vector<common::table_id_t> getTableIDs() const { return nodeTableIDs; }
    inline binder::expression_vector getProperties() const { return properties; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return make_unique<LogicalScanNodeProperty>(
            nodeID, nodeTableIDs, properties, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> nodeID;
    std::vector<common::table_id_t> nodeTableIDs;
    binder::expression_vector properties;
};

} // namespace planner
} // namespace kuzu
