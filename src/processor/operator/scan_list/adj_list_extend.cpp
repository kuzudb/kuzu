#include "processor/operator/scan_list/adj_list_extend.h"

namespace kuzu {
namespace processor {

void AdjListExtend::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanList::initLocalStateInternal(resultSet, context);
    resultSet->initListSyncState(outDataPos.dataChunkPos);
    listHandle = make_shared<ListHandle>(*resultSet->getListSyncState(outDataPos.dataChunkPos));
}

bool AdjListExtend::getNextTuplesInternal() {
    if (listHandle->listSyncState.hasMoreToRead()) {
        lists->readValues(outValueVector, *listHandle);
        metrics->numOutputTuple.increase(outValueVector->state->selVector->selectedSize);
        return true;
    }
    do {
        if (!children[0]->getNextTuple()) {
            return false;
        }
        auto currentIdx = inValueVector->state->getPositionOfCurrIdx();
        if (inValueVector->isNull(currentIdx)) {
            outValueVector->state->selVector->selectedSize = 0;
            continue;
        }
        ((AdjLists*)lists)
            ->initListReadingState(
                inValueVector->readNodeOffset(inValueVector->state->getPositionOfCurrIdx()),
                *listHandle, transaction->getType());
        lists->readValues(outValueVector, *listHandle);
    } while (outValueVector->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(outValueVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
