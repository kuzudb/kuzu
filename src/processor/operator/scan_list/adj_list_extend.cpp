#include "processor/operator/scan_list/adj_list_extend.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> AdjListExtend::init(ExecutionContext* context) {
    resultSet = ScanList::init(context);
    outValueVector = make_shared<ValueVector>(NODE_ID, context->memoryManager);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    resultSet->insert(outDataPos.dataChunkPos, make_shared<ListSyncState>());
    listHandle = make_shared<ListHandle>(*resultSet->getListSyncState(outDataPos.dataChunkPos));
    return resultSet;
}

bool AdjListExtend::getNextTuplesInternal() {
    if (listHandle->listSyncState.hasMoreToRead()) {
        lists->readValues(outValueVector, *listHandle);
        metrics->numOutputTuple.increase(outDataChunk->state->selVector->selectedSize);
        return true;
    }
    do {
        if (!children[0]->getNextTuple()) {
            return false;
        }
        auto currentIdx = inDataChunk->state->getPositionOfCurrIdx();
        if (inValueVector->isNull(currentIdx)) {
            outValueVector->state->selVector->selectedSize = 0;
            continue;
        }
        ((AdjLists*)lists)
            ->initListReadingState(
                inValueVector->readNodeOffset(inDataChunk->state->getPositionOfCurrIdx()),
                *listHandle, transaction->getType());
        lists->readValues(outValueVector, *listHandle);
    } while (outDataChunk->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(outDataChunk->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
