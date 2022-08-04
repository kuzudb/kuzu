#include "src/processor/include/physical_plan/operator/scan_column/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ScanUnstructuredProperty::init(ExecutionContext* context) {
    resultSet = BaseScanColumn::init(context);
    for (auto i = 0u; i < outputVectorsPos.size(); ++i) {
        assert(unstructuredPropertyLists->getDataTypeId() == UNSTRUCTURED);
        auto vector = make_shared<ValueVector>(UNSTRUCTURED, context->memoryManager);
        inputNodeIDDataChunk->insert(outputVectorsPos[i].valueVectorPos, vector);
        outputVectors.push_back(vector);
        propertyKeyToResultVectorMap.insert({propertyKeys[i], vector.get()});
    }
    return resultSet;
}

bool ScanUnstructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    for (auto& vector : outputVectors) {
        vector->resetOverflowBuffer();
    }
    unstructuredPropertyLists->readProperties(
        transaction, inputNodeIDVector.get(), propertyKeyToResultVectorMap);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
