#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/store/table.h"

namespace kuzu {
namespace processor {

class ScanTable : public PhysicalOperator {
public:
    ScanTable(PhysicalOperatorType operatorType, const DataPos& nodeIDPos,
        std::vector<DataPos> outVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramString)
        : PhysicalOperator{operatorType, std::move(child), id, paramString}, nodeIDPos{nodeIDPos},
          outVectorsPos{std::move(outVectorsPos)}, nodeIDVector{nullptr}, outState{nullptr} {}

    ScanTable(PhysicalOperatorType operatorType, const DataPos& nodeIDPos,
        std::vector<DataPos> outVectorsPos, uint32_t id, const std::string& paramString)
        : PhysicalOperator{operatorType, id, paramString}, nodeIDPos{nodeIDPos},
          outVectorsPos{std::move(outVectorsPos)}, nodeIDVector{nullptr}, outState{nullptr} {}

protected:
    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    void initVectors(storage::TableScanState& state, const ResultSet& resultSet) const;

protected:
    // Input node id vector position.
    DataPos nodeIDPos;
    // Output vector (properties or CSRs) positions
    std::vector<DataPos> outVectorsPos;
    // Node id vector.
    common::ValueVector* nodeIDVector;
    // All output vectors share the same state.
    common::DataChunkState* outState;
};

} // namespace processor
} // namespace kuzu
