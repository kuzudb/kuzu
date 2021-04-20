#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/processor/include/task_system/morsel.h"
#include "src/storage/include/structures/lists.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanUnstructuredProperty : public ScanAttribute {

public:
    ScanUnstructuredProperty(uint64_t dataChunkPos, uint64_t valueVectorPos, uint32_t propertyKey,
        BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator);

    ~ScanUnstructuredProperty() { lists->reclaim(handle); }

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanUnstructuredProperty>(
            dataChunkPos, valueVectorPos, propertyKey, lists, prevOperator->clone());
    }

protected:
    uint32_t propertyKey;
    Lists<UNKNOWN>* lists;
};

} // namespace processor
} // namespace graphflow
