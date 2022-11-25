#include "processor/operator/scan_column/scan_column.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> BaseScanColumn::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    inputNodeIDDataChunk = resultSet->dataChunks[inputNodeIDVectorPos.dataChunkPos];
    inputNodeIDVector = inputNodeIDDataChunk->valueVectors[inputNodeIDVectorPos.valueVectorPos];
    return resultSet;
}

shared_ptr<ResultSet> ScanMultipleColumns::init(ExecutionContext* context) {
    resultSet = BaseScanColumn::init(context);
    for (auto i = 0u; i < outVectorsPos.size(); ++i) {
        auto vector = make_shared<ValueVector>(outDataTypes[i], context->memoryManager);
        inputNodeIDDataChunk->insert(outVectorsPos[i].valueVectorPos, vector);
        outVectors.push_back(vector);
    }
    return resultSet;
}

} // namespace processor
} // namespace kuzu
