#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

AdjListExtend::AdjListExtend(uint64_t inDataChunkPos, uint64_t inValueVectorPos, AdjLists* lists,
    label_t outNodeIDVectorLabel, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : ReadList{inDataChunkPos, inValueVectorPos, lists, move(prevOperator), context, id},
      outNodeIDVectorLabel{outNodeIDVectorLabel} {
    outValueVector =
        make_shared<NodeIDVector>(outNodeIDVectorLabel, lists->getNodeIDCompressionScheme(), false);
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(outValueVector);
    auto listSyncState = make_shared<ListSyncState>();
    resultSet->append(outDataChunk, listSyncState);
    listsPageHandle->setListSyncState(listSyncState);
    listsPageHandle->setIsAdjListHandle();
}

void AdjListExtend::getNextTuples() {
    metrics->executionTime.start();
    if (listsPageHandle->hasMoreToRead()) {
        readValuesFromList();
        return;
    }
    while (true) {
        prevOperator->getNextTuples();
        if (inDataChunk->state->size > 0) {
            readValuesFromList();
            if (outDataChunk->state->size > 0) {
                break;
            }
        } else {
            outDataChunk->state->size = 0;
            break;
        }
    }
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outDataChunk->state->size);
}

} // namespace processor
} // namespace graphflow
