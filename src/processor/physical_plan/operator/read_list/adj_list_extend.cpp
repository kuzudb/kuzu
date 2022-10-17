#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> AdjListExtend::init(ExecutionContext* context) {
    resultSet = ReadList::init(context);
    outValueVector = make_shared<ValueVector>(NODE_ID, context->memoryManager);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    resultSet->insert(outDataPos.dataChunkPos, make_shared<ListSyncState>());
    listHandle = make_shared<ListHandle>(*resultSet->getListSyncState(outDataPos.dataChunkPos));
    return resultSet;
}

bool AdjListExtend::getNextTuples() {
    metrics->executionTime.start();
    if (listHandle->listSyncState.hasMoreToRead()) {
        listsWithAdjAndPropertyListsUpdateStore->readValues(outValueVector, *listHandle);
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(outDataChunk->state->selVector->selectedSize);
        return true;
    }
    do {
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        auto currentIdx = inDataChunk->state->getPositionOfCurrIdx();
        if (inValueVector->isNull(currentIdx)) {
            outValueVector->state->selVector->selectedSize = 0;
            continue;
        }
        ((AdjLists*)listsWithAdjAndPropertyListsUpdateStore)
            ->initListReadingState(
                inValueVector->readNodeOffset(inDataChunk->state->getPositionOfCurrIdx()),
                *listHandle, transaction->getType());
        listsWithAdjAndPropertyListsUpdateStore->readValues(outValueVector, *listHandle);
    } while (outDataChunk->state->selVector->selectedSize == 0);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outDataChunk->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
