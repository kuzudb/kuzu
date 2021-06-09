#include "src/processor/include/physical_plan/operator/crud/crud.h"

namespace graphflow {
namespace processor {

CRUDOperator::CRUDOperator(uint64_t dataChunkPos, PhysicalOperatorType type,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), type, context, id}, dataChunkPos{dataChunkPos} {
    inDataChunk = resultSet->dataChunks[dataChunkPos];
}

void CRUDOperator::getNextTuples() {
    while (true) {
        prevOperator->getNextTuples();
        if (inDataChunk->state->size == 0) {
            break;
        }
        context.transaction->localStorage.addDataChunk(inDataChunk.get());
    }
}

} // namespace processor
} // namespace graphflow
