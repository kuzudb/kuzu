#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/store/table.h"

namespace kuzu {
namespace processor {

struct ScanTableInfo {
    // Node/Rel ID vector position.
    DataPos IDPos;
    // Output vector (properties or CSRs) positions
    std::vector<DataPos> outVectorsPos;

    ScanTableInfo(DataPos nodeIDPos, std::vector<DataPos> outVectorsPos)
        : IDPos{nodeIDPos}, outVectorsPos{std::move(outVectorsPos)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ScanTableInfo);

private:
    ScanTableInfo(const ScanTableInfo& other)
        : IDPos{other.IDPos}, outVectorsPos{other.outVectorsPos} {}
};

class ScanTable : public PhysicalOperator {
public:
    ScanTable(PhysicalOperatorType operatorType, ScanTableInfo info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{operatorType, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, IDVector{nullptr}, outState{nullptr} {}

    ScanTable(PhysicalOperatorType operatorType, ScanTableInfo info, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{operatorType, id, std::move(printInfo)}, info{std::move(info)},
          IDVector{nullptr}, outState{nullptr} {}

protected:
    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    virtual void initVectors(storage::TableScanState& state, const ResultSet& resultSet) const;

protected:
    ScanTableInfo info;
    // Node/Rel id vector.
    common::ValueVector* IDVector;
    // All output vectors share the same state.
    common::DataChunkState* outState;
};

} // namespace processor
} // namespace kuzu
