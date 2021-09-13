#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/storage/include/data_structure/lists/unstructured_property_lists.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanUnstructuredProperty : public ScanAttribute {

public:
    ScanUnstructuredProperty(const DataPos& inDataPos, const DataPos& outDataPos,
        uint32_t propertyKey, UnstructuredPropertyLists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ScanAttribute{inDataPos, outDataPos, move(prevOperator), context, id},
          propertyKey{propertyKey}, lists{lists} {}

    ~ScanUnstructuredProperty(){};

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanUnstructuredProperty>(
            inDataPos, outDataPos, propertyKey, lists, prevOperator->clone(), context, id);
    }

protected:
    uint32_t propertyKey;
    UnstructuredPropertyLists* lists;
};

} // namespace processor
} // namespace graphflow
