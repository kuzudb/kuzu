#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> AdjListExtend::init(ExecutionContext* context) {
    resultSet = ReadList::init(context);
    outValueVector = make_shared<ValueVector>(NODE_ID, context->memoryManager);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    auto listSyncState = make_shared<ListSyncState>();
    resultSet->insert(outDataPos.dataChunkPos, listSyncState);
    largeListHandle->setListSyncState(listSyncState);
    return resultSet;
}

bool AdjListExtend::getNextTuples() {
    metrics->executionTime.start();
    if (largeListHandle->hasMoreToRead()) {
        readValuesFromList();
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(outDataChunk->state->selVector->selectedSize);
        return true;
    }
    do {
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        readValuesFromList();
    } while (outDataChunk->state->selVector->selectedSize == 0);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outDataChunk->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
