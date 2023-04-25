#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    if (inNodeIDVectorPos.dataChunkPos == common::INVALID_VECTOR_IDX) {
        return;
    }
    inNodeIDVector = resultSet->getValueVector(inNodeIDVectorPos).get();
    for (auto& dataPos : outputVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos);
        outputVectors.push_back(vector.get());
    }
}

} // namespace processor
} // namespace kuzu
