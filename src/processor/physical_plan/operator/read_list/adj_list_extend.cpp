#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
AdjListExtend<IS_OUT_DATACHUNK_FILTERED>::AdjListExtend(uint64_t inDataChunkPos,
    uint64_t inValueVectorPos, AdjLists* lists, unique_ptr<PhysicalOperator> prevOperator,
    ExecutionContext& context, uint32_t id)
    : ReadList{inDataChunkPos, inValueVectorPos, lists, move(prevOperator), context, id} {
    outValueVector = make_shared<NodeIDVector>(0, lists->getNodeIDCompressionScheme(), false);
    outDataChunk = make_shared<DataChunk>(!IS_OUT_DATACHUNK_FILTERED);
    outDataChunk->append(outValueVector);
    auto listSyncState = make_shared<ListSyncState>();
    resultSet->append(outDataChunk, listSyncState);
    listsPageHandle->setListSyncState(listSyncState);
    listsPageHandle->setIsAdjListHandle();
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void AdjListExtend<IS_OUT_DATACHUNK_FILTERED>::getNextTuples() {
    metrics->executionTime.start();
    if (listsPageHandle->hasMoreToRead()) {
        readValuesFromList();
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            outDataChunk->state->resetSelector();
        }
        return;
    }
    while (true) {
        prevOperator->getNextTuples();
        if (inDataChunk->state->size > 0) {
            readValuesFromList();
            if (outDataChunk->state->size > 0) {
                if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                    outDataChunk->state->resetSelector();
                }
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

template class AdjListExtend<true>;
template class AdjListExtend<false>;
} // namespace processor
} // namespace graphflow
