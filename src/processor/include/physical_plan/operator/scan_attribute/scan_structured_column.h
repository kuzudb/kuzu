#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/processor/include/task_system/morsel.h"
#include "src/storage/include/structures/column.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanStructuredColumn : public ScanAttribute {

public:
    ScanStructuredColumn(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
        unique_ptr<PhysicalOperator> prevOperator);

    ~ScanStructuredColumn() { column->reclaim(handle); }

    void getNextTuples() override;

protected:
    BaseColumn* column;
};

} // namespace processor
} // namespace graphflow
