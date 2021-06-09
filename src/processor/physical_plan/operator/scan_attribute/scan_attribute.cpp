#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"

namespace graphflow {
namespace processor {

ScanAttribute::ScanAttribute(uint64_t dataChunkPos, uint64_t valueVectorPos,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), SCAN_ATTRIBUTE, context, id}, dataChunkPos{dataChunkPos},
      valueVectorPos{valueVectorPos}, handle{make_unique<DataStructureHandle>()} {
    resultSet = this->prevOperator->getResultSet();
    inDataChunk = resultSet->dataChunks[dataChunkPos];
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
}

} // namespace processor
} // namespace graphflow
