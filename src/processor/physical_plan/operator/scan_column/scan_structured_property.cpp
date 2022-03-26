#include "src/processor/include/physical_plan/operator/scan_column/scan_structured_property.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ScanStructuredProperty::initResultSet() {
    resultSet = BaseScanColumn::initResultSet();
    assert(outputVectorsPos.size() == propertyColumns.size());
    for (auto i = 0u; i < propertyColumns.size(); ++i) {
        auto vector = make_shared<ValueVector>(context.memoryManager, propertyColumns[i]->dataType);
        inputNodeIDDataChunk->insert(outputVectorsPos[i].valueVectorPos, vector);
        outputVectors.push_back(vector);
    }
    return resultSet;
}

bool ScanStructuredProperty::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    for (auto i = 0u; i < propertyColumns.size(); ++i) {
        outputVectors[i]->resetOverflowBuffer();
        propertyColumns[i]->readValues(inputNodeIDVector, outputVectors[i]);
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
