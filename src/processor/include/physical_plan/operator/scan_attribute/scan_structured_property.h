#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

class ScanStructuredProperty : public ScanColumn {

public:
    ScanStructuredProperty(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanStructuredProperty>(
            dataChunkPos, valueVectorPos, column, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
