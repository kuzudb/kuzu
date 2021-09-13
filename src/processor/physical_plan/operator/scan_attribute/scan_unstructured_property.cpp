#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

void ScanUnstructuredProperty::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ScanAttribute::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    inDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
}

void ScanUnstructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    prevOperator->getNextTuples();
    if (inDataChunk->state->selectedSize > 0) {
        lists->readUnstructuredProperties(
            inValueVector, propertyKey, outValueVector, *metrics->bufferManagerMetrics);
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
