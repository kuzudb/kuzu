#pragma once

#include "processor/operator/physical_operator.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

class ScanColumns : public PhysicalOperator {
protected:
    ScanColumns(const DataPos& inVectorPos, vector<DataPos> outPropertyVectorsPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_PROPERTY, std::move(child), id,
              paramsString},
          inputNodeIDVectorPos{inVectorPos}, outPropertyVectorsPos{
                                                 std::move(outPropertyVectorsPos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    DataPos inputNodeIDVectorPos;
    shared_ptr<ValueVector> inputNodeIDVector;
    vector<DataPos> outPropertyVectorsPos;
    vector<shared_ptr<ValueVector>> outPropertyVectors;
};

} // namespace processor
} // namespace kuzu
