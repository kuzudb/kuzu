#pragma once

#include "src/processor/include/physical_plan/operator/scan_column/scan_column.h"

namespace graphflow {
namespace processor {

class AdjColumnExtend : public ScanColumn {

public:
    AdjColumnExtend(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
        unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjColumnExtend>(
            dataChunkPos, valueVectorPos, column, prevOperator->clone());
    }

private:
    uint64_t prevInDataChunkNumSelectedValues = 0ul;
};

} // namespace processor
} // namespace graphflow
