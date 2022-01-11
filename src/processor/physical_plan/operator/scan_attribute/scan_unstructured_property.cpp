#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ScanUnstructuredProperty::initResultSet() {
    ScanAttribute::initResultSet();
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    inDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    return resultSet;
}

bool ScanUnstructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    outValueVector->resetStringBuffer();
    lists->readUnstructuredProperties(
        inValueVector, propertyKey, outValueVector, *metrics->bufferManagerMetrics);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
