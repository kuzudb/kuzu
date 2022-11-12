#include "include/scan_column.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> BaseScanColumn::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    inputNodeIDDataChunk = resultSet->dataChunks[inputNodeIDVectorPos.dataChunkPos];
    inputNodeIDVector = inputNodeIDDataChunk->valueVectors[inputNodeIDVectorPos.valueVectorPos];
    return resultSet;
}

} // namespace processor
} // namespace kuzu
