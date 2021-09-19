#include "src/processor/include/physical_plan/operator/crud/crud.h"

namespace graphflow {
namespace processor {

CRUDOperator::CRUDOperator(uint64_t dataChunkPos, PhysicalOperatorType type,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{move(prevOperator), type, context, id}, dataChunkPos{dataChunkPos} {
    inDataChunk = resultSet->dataChunks[dataChunkPos];
}

bool CRUDOperator::getNextTuples() {
    while (true) {
        if (!prevOperator->getNextTuples()) {
            return false;
        }
        context.transaction->localStorage.addDataChunk(inDataChunk.get());
    }
}

} // namespace processor
} // namespace graphflow
