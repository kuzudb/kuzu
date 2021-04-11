#pragma once

#include "src/processor/include/physical_plan/operator/scan_column/scan_column.h"

namespace graphflow {
namespace processor {

class ScanProperty : public ScanColumn {

public:
    ScanProperty(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
        unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanProperty>(
            dataChunkPos, valueVectorPos, column, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
