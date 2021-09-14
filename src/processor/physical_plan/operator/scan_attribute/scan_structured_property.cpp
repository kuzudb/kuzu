#include "src/processor/include/physical_plan/operator/scan_attribute/scan_structured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

void ScanStructuredProperty::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ScanColumn::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, column->getDataType());
    inDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
}

void ScanStructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    ScanColumn::getNextTuples();
    outValueVector->fillNullMask();
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
