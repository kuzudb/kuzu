#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class ScanColumns : public PhysicalOperator {
protected:
    ScanColumns(const DataPos& inVectorPos, std::vector<DataPos> outPropertyVectorsPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_PROPERTY, std::move(child), id,
              paramsString},
          inputNodeIDVectorPos{inVectorPos}, outPropertyVectorsPos{
                                                 std::move(outPropertyVectorsPos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    DataPos inputNodeIDVectorPos;
    common::ValueVector* inputNodeIDVector;
    std::vector<DataPos> outPropertyVectorsPos;
    std::vector<common::ValueVector*> outPropertyVectors;
};

} // namespace processor
} // namespace kuzu
