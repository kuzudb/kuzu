#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

void AdjListExtend::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ReadList::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    auto listSyncState = make_shared<ListSyncState>();
    resultSet->insert(outDataPos.dataChunkPos, listSyncState);
    largeListHandle->setListSyncState(listSyncState);
}

bool AdjListExtend::getNextTuples() {
    metrics->executionTime.start();
    if (largeListHandle->hasMoreToRead()) {
        readValuesFromList();
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
        return true;
    }
    do {
        if (!prevOperator->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        readValuesFromList();
    } while (outDataChunk->state->selectedSize == 0);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
