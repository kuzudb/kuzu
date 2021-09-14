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
