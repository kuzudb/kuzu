#include "src/processor/include/physical_plan/operator/scan_column/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ScanUnstructuredProperty::initResultSet() {
    resultSet = BaseScanColumn::initResultSet();
    for (auto i = 0u; i < outputVectorsPos.size(); ++i) {
        assert(unstructuredPropertyLists->getDataType() == UNSTRUCTURED);
        auto vector = make_shared<ValueVector>(context.memoryManager, UNSTRUCTURED);
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
        vector->resetStringBuffer();
    }
    unstructuredPropertyLists->readProperties(
        inputNodeIDVector.get(), propertyKeyToResultVectorMap);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
