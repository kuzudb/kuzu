#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

class AdjColumnExtend : public ScanColumn {

public:
    AdjColumnExtend(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjColumnExtend>(
            dataChunkPos, valueVectorPos, column, prevOperator->clone(), context, id);
    }

private:
    uint64_t prevInNumSelectedValues;
};

} // namespace processor
} // namespace graphflow
