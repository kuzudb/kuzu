#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/storage/include/data_structure/lists/lists.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanUnstructuredProperty : public ScanAttribute {

public:
    ScanUnstructuredProperty(uint64_t dataChunkPos, uint64_t valueVectorPos, uint32_t propertyKey,
        BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context,
        uint32_t id);

    ~ScanUnstructuredProperty() { lists->reclaim(handle); }

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanUnstructuredProperty>(
            dataChunkPos, valueVectorPos, propertyKey, lists, prevOperator->clone(), context, id);
    }

protected:
    uint32_t propertyKey;
    Lists<UNSTRUCTURED>* lists;
};

} // namespace processor
} // namespace graphflow
