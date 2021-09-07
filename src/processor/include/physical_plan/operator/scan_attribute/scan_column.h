#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/storage/include/data_structure/column.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanColumn : public ScanAttribute {

public:
    ScanColumn(uint64_t dataChunkPos, uint64_t valueVectorPos, Column* column,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ScanAttribute{dataChunkPos, valueVectorPos, move(prevOperator), context, id},
          column{column} {};
    ~ScanColumn() override{};
    void getNextTuples() override;

protected:
    Column* column;
};

} // namespace processor
} // namespace graphflow
