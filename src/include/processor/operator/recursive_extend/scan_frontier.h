#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class ScanFrontier : public PhysicalOperator {
public:
    ScanFrontier(DataPos nodeIDVectorPos, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramsString},
          nodeIDVectorPos{nodeIDVectorPos} {}

    inline bool isSource() const override { return true; }

    inline void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override {
        nodeIDVector = resultSet->getValueVector(nodeIDVectorPos);
    }

    bool getNextTuplesInternal(kuzu::processor::ExecutionContext* context) override;

    inline void setNodeID(common::nodeID_t nodeID) {
        nodeIDVector->setValue<common::nodeID_t>(0, nodeID);
        hasExecuted = false;
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanFrontier>(nodeIDVectorPos, id, paramsString);
    }

private:
    DataPos nodeIDVectorPos;
    std::shared_ptr<common::ValueVector> nodeIDVector;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
