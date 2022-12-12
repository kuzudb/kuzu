#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class BaseExtendAndScanRelProperties : public PhysicalOperator {
protected:
    BaseExtendAndScanRelProperties(PhysicalOperatorType operatorType,
        const DataPos& inNodeIDVectorPos, const DataPos& outNodeIDVectorPos,
        vector<DataPos> outPropertyVectorsPos, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{operatorType, std::move(child), id, paramsString},
          inNodeIDVectorPos{inNodeIDVectorPos}, outNodeIDVectorPos{outNodeIDVectorPos},
          outPropertyVectorsPos{std::move(outPropertyVectorsPos)} {}
    virtual ~BaseExtendAndScanRelProperties() override = default;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    // vector positions
    DataPos inNodeIDVectorPos;
    DataPos outNodeIDVectorPos;
    vector<DataPos> outPropertyVectorsPos;
    // vectors
    shared_ptr<ValueVector> inNodeIDVector;
    shared_ptr<ValueVector> outNodeIDVector;
    vector<shared_ptr<ValueVector>> outPropertyVectors;
};

} // namespace processor
} // namespace kuzu
