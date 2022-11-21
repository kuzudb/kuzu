#include "processor/operator/scan_column/scan_structured_property.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> ScanStructuredProperty::init(ExecutionContext* context) {
    resultSet = BaseScanColumn::init(context);
    assert(outputVectorsPos.size() == propertyColumns.size());
    for (auto i = 0u; i < propertyColumns.size(); ++i) {
        auto vector =
            make_shared<ValueVector>(propertyColumns[i]->dataType, context->memoryManager);
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
        propertyColumns[i]->read(transaction, inputNodeIDVector, outputVectors[i]);
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace kuzu
