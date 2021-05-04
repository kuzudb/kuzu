#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"

namespace graphflow {
namespace processor {

ScanAttribute::ScanAttribute(
    uint64_t dataChunkPos, uint64_t valueVectorPos, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), SCAN_ATTRIBUTE}, dataChunkPos{dataChunkPos},
      valueVectorPos{valueVectorPos} {
    dataChunks = this->prevOperator->getDataChunks();
    inDataChunk = dataChunks->getDataChunk(dataChunkPos);
    inNodeIDVector = static_pointer_cast<NodeIDVector>(inDataChunk->getValueVector(valueVectorPos));
    handle = make_unique<DataStructureHandle>();
}

} // namespace processor
} // namespace graphflow
