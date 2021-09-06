#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

AdjListExtend::AdjListExtend(uint64_t inDataChunkPos, uint64_t inValueVectorPos, AdjLists* lists,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : ReadList{inDataChunkPos, inValueVectorPos, lists, move(prevOperator), context, id,
          true /* is adj list */} {
    outValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(outValueVector);
    auto listSyncState = make_shared<ListSyncState>();
    resultSet->append(outDataChunk, listSyncState);
    largeListHandle->setListSyncState(listSyncState);
}

void AdjListExtend::getNextTuples() {
    metrics->executionTime.start();
    if (largeListHandle->hasMoreToRead()) {
        readValuesFromList();
        return;
    }
    while (true) {
        prevOperator->getNextTuples();
        if (inDataChunk->state->selectedSize > 0) {
            readValuesFromList();
            if (outDataChunk->state->selectedSize > 0) {
                break;
            }
        } else {
            outDataChunk->state->selectedSize = 0;
            break;
        }
    }
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
}

} // namespace processor
} // namespace graphflow
