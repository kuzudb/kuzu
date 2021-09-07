#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/storage/include/data_structure/lists/unstructured_property_lists.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanUnstructuredProperty : public ScanAttribute {

public:
    ScanUnstructuredProperty(uint32_t inAndOutDataChunkPos, uint32_t inValueVectorPos,
        uint32_t outValueVectorPos, uint32_t propertyKey, UnstructuredPropertyLists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);
    ~ScanUnstructuredProperty(){};
    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanUnstructuredProperty>(inAndOutDataChunkPos, inValueVectorPos,
            outValueVectorPos, propertyKey, lists, prevOperator->clone(), context, id);
    }

protected:
    uint32_t propertyKey;
    UnstructuredPropertyLists* lists;
};

} // namespace processor
} // namespace graphflow
