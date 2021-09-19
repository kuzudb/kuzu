#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

void ScanStructuredProperty::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ScanAttribute::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, column->getDataType());
    inDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
}

bool ScanStructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    if (!prevOperator->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    column->readValues(inValueVector, outValueVector, *metrics->bufferManagerMetrics);
    outValueVector->fillNullMask();
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
