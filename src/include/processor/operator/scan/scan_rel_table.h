#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class ScanRelTable : public PhysicalOperator {
protected:
    ScanRelTable(const DataPos& inNodeIDVectorPos, std::vector<DataPos> outputVectorsPos,
        PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{operatorType, std::move(child), id, paramsString},
          inNodeIDVectorPos{inNodeIDVectorPos}, outputVectorsPos{std::move(outputVectorsPos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    // vector positions
    DataPos inNodeIDVectorPos;
    std::vector<DataPos> outputVectorsPos;
    // vectors
    common::ValueVector* inNodeIDVector;
    std::vector<common::ValueVector*> outputVectors;
};

} // namespace processor
} // namespace kuzu
