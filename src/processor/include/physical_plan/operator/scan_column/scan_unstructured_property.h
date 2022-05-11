#pragma once

#include "src/processor/include/physical_plan/operator/scan_column/scan_column.h"
#include "src/storage/include/storage_structure/lists/unstructured_property_lists.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

// Although scan unstructured property is reading from list, it's access is aligned to reading from
// multiple column with non-fixed length. See unstructured_property_lists.h for details.
class ScanUnstructuredProperty : public ScanMultipleColumns {

public:
    ScanUnstructuredProperty(const DataPos& inputNodeIDVectorPos,
        vector<DataPos> outputPropertyVectorsPos, vector<uint32_t> propertyKeys,
        UnstructuredPropertyLists* unstructuredPropertyLists, unique_ptr<PhysicalOperator> child,
        uint32_t id)
        : ScanMultipleColumns{inputNodeIDVectorPos, move(outputPropertyVectorsPos), move(child),
              id},
          propertyKeys{move(propertyKeys)}, unstructuredPropertyLists{unstructuredPropertyLists} {}

    ~ScanUnstructuredProperty() = default;

    PhysicalOperatorType getOperatorType() override { return SCAN_UNSTRUCTURED_PROPERTY; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanUnstructuredProperty>(inputNodeIDVectorPos, outputVectorsPos,
            propertyKeys, unstructuredPropertyLists, children[0]->clone(), id);
    }

private:
    vector<uint32_t> propertyKeys;
    UnstructuredPropertyLists* unstructuredPropertyLists;

    unordered_map<uint32_t, ValueVector*> propertyKeyToResultVectorMap;
};

} // namespace processor
} // namespace graphflow
