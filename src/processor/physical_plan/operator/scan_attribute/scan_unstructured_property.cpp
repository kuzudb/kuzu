#include "src/processor/include/physical_plan/operator/scan_attribute/scan_unstructured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

void ScanUnstructuredProperty::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ScanAttribute::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, lists->getDataType());
    inDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
}

bool ScanUnstructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    if (!prevOperator->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    lists->readUnstructuredProperties(
        inValueVector, propertyKey, outValueVector, *metrics->bufferManagerMetrics);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
