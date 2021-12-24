#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ScanStructuredProperty::initResultSet() {
    ScanAttribute::initResultSet();
    outValueVector = make_shared<ValueVector>(context.memoryManager, column->getDataType());
    inDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    return resultSet;
}

bool ScanStructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    if (!prevOperator->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    column->readValues(inValueVector, outValueVector, *metrics->bufferManagerMetrics);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
