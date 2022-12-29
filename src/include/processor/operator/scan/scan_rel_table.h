#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class ScanRelTable : public PhysicalOperator {
protected:
    ScanRelTable(const DataPos& inNodeIDVectorPos, vector<DataPos> outputVectorsPos,
        PhysicalOperatorType operatorType, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{operatorType, std::move(child), id, paramsString},
          inNodeIDVectorPos{inNodeIDVectorPos}, outputVectorsPos{std::move(outputVectorsPos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    // vector positions
    DataPos inNodeIDVectorPos;
    vector<DataPos> outputVectorsPos;
    // vectors
    shared_ptr<ValueVector> inNodeIDVector;
    vector<shared_ptr<ValueVector>> outputVectors;
};

} // namespace processor
} // namespace kuzu
