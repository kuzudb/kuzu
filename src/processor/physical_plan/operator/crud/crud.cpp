#include "src/processor/include/physical_plan/operator/crud/crud.h"

namespace graphflow {
namespace processor {

CRUDOperator::CRUDOperator(uint64_t dataChunkPos, Transaction* transactionPtr,
    PhysicalOperatorType type, unique_ptr<PhysicalOperator> prevOperator)
    : PhysicalOperator{move(prevOperator), type}, transactionPtr{transactionPtr}, graph{graph},
      dataChunkPos{dataChunkPos} {
    inDataChunk = resultSet->dataChunks[dataChunkPos];
}

void CRUDOperator::getNextTuples() {
    while (true) {
        prevOperator->getNextTuples();
        if (inDataChunk->state->size == 0) {
            break;
        }
        transactionPtr->localStorage.addDataChunk(inDataChunk.get());
    }
}

} // namespace processor
} // namespace graphflow
