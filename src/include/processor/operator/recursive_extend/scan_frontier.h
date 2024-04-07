#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct ScanFrontierInfo {
    DataPos nodeIDPos;
    DataPos flagPos;
};

class ScanFrontier : public PhysicalOperator {
public:
    ScanFrontier(ScanFrontierInfo info, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramsString}, info{info} {}

    inline bool isSource() const override { return true; }

    inline void initLocalStateInternal(ResultSet* /*resultSet_*/,
        ExecutionContext* /*context*/) override {
        nodeIDVector = resultSet->getValueVector(info.nodeIDPos).get();
        flagVector = resultSet->getValueVector(info.flagPos).get();
    }

    bool getNextTuplesInternal(kuzu::processor::ExecutionContext* context) override;

    inline void setNodeID(common::nodeID_t nodeID) {
        nodeIDVector->setValue<common::nodeID_t>(0, nodeID);
        hasExecuted = false;
    }
    void setNodePredicateExecFlag(bool flag) { flagVector->setValue<bool>(0, flag); }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanFrontier>(info, id, paramsString);
    }

private:
    ScanFrontierInfo info;
    common::ValueVector* nodeIDVector;
    common::ValueVector* flagVector;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
