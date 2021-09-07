#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

class ScanStructuredProperty : public ScanColumn {

public:
    ScanStructuredProperty(uint32_t inAndOutDataChunkPos, uint32_t inValueVectorPos,
        uint32_t outValueVectorPos, Column* column, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanStructuredProperty>(inAndOutDataChunkPos, inValueVectorPos,
            outValueVectorPos, column, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
