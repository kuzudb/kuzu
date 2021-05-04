#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
AdjListExtend<IS_OUT_DATACHUNK_FILTERED>::AdjListExtend(uint64_t inDataChunkPos,
    uint64_t inValueVectorPos, AdjLists* lists, unique_ptr<PhysicalOperator> prevOperator)
    : ReadList{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)} {
    outValueVector = make_shared<NodeIDVector>(0, lists->getNodeIDCompressionScheme(), false);
    outDataChunk = make_shared<DataChunk>(true /* initializeSelectedValuesPos */);
    outDataChunk->appendAndSetVectorState(outValueVector);
    auto listSyncState = make_shared<ListSyncState>();
    dataChunks->append(outDataChunk, listSyncState);
    handle->setListSyncState(listSyncState);
    handle->setIsAdjListHandle();
}

template<bool IS_OUT_DATACHUNK_FILTERED>
void AdjListExtend<IS_OUT_DATACHUNK_FILTERED>::getNextTuples() {
    if (handle->hasMoreToRead()) {
        readValuesFromList();
        outDataChunk->state->numSelectedValues = outDataChunk->state->size;
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            outDataChunk->state->initializeSelector();
        }
        return;
    }
    while (true) {
        do {
            prevOperator->getNextTuples();
        } while (inDataChunk->state->size > 0 && inDataChunk->state->numSelectedValues == 0);
        if (inDataChunk->state->size > 0) {
            readValuesFromList();
            if (outDataChunk->state->size > 0) {
                outDataChunk->state->numSelectedValues = outDataChunk->state->size;
                if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                    outDataChunk->state->initializeSelector();
                }
                return;
            }
        } else {
            outDataChunk->state->size = outDataChunk->state->numSelectedValues = 0;
            return;
        }
    }
}

template class AdjListExtend<true>;
template class AdjListExtend<false>;
} // namespace processor
} // namespace graphflow
