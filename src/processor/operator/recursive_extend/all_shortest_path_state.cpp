#include "processor/operator/recursive_extend/all_shortest_path_state.h"

namespace kuzu {
namespace processor {

#if defined(__GNUC__) || defined(__GNUG__)
template<>
void AllShortestPathMorsel<false>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST) {
            if (__sync_bool_compare_and_swap(
                    &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_DST_NEW)) {
                __sync_bool_compare_and_swap(&bfsSharedState->pathLength[nodeID.offset], 0u,
                    bfsSharedState->currentLevel + 1);
                numVisitedDstNodes++;
                if (bfsSharedState->minDistance < bfsSharedState->currentLevel) {
                    __sync_bool_compare_and_swap(&bfsSharedState->minDistance,
                        bfsSharedState->minDistance, bfsSharedState->currentLevel);
                }
            }
        } else if (state == NOT_VISITED) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_NEW);
        }
        state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == VISITED_NEW || state == VISITED_DST_NEW) {
            auto currMultiplicity = bfsSharedState->nodeIDToMultiplicity[nodeID.offset];
            while (
                !__sync_bool_compare_and_swap(&bfsSharedState->nodeIDToMultiplicity[nodeID.offset],
                    currMultiplicity, currMultiplicity + boundNodeMultiplicity)) {
                currMultiplicity = bfsSharedState->nodeIDToMultiplicity[nodeID.offset];
            }
        }
    }
}
#endif

#if defined(__GNUC__) || defined(__GNUG__)
template<>
void AllShortestPathMorsel<true>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) {}
#endif

template<>
int64_t AllShortestPathMorsel<false>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) {
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    while (startScanIdxAndSize.first < endIdx && size < common::DEFAULT_VECTOR_CAPACITY) {
        if ((bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST ||
                bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW) &&
            bfsSharedState->pathLength[startScanIdxAndSize.first] >= bfsSharedState->lowerBound) {
            auto multiplicity = bfsSharedState->nodeIDToMultiplicity[startScanIdxAndSize.first];
            do {
                dstNodeIDVector->setValue<common::nodeID_t>(
                    size, common::nodeID_t{startScanIdxAndSize.first, tableID});
                pathLengthVector->setValue<int64_t>(
                    size, bfsSharedState->pathLength[startScanIdxAndSize.first]);
                size++;
            } while (--multiplicity && size < common::DEFAULT_VECTOR_CAPACITY);
            /// ValueVector capacity was reached, keep the morsel state saved and exit from loop.
            if (multiplicity > 0) {
                bfsSharedState->nodeIDToMultiplicity[startScanIdxAndSize.first] = multiplicity;
                break;
            }
        }
        startScanIdxAndSize.first++;
    }
    prevDistMorselStartEndIdx = {startScanIdxAndSize.first, endIdx};
    if (size > 0) {
        dstNodeIDVector->state->initOriginalAndSelectedSize(size);
        // We need to rescan the FTable to get the source for which the pathLengths were computed.
        // This is because the thread that scanned FTable initially might not be the thread writing
        // the pathLengths to its vector.
        inputFTableSharedState->getTable()->scan(vectorsToScan, bfsSharedState->inputFTableTupleIdx,
            1 /* numTuples */, colIndicesToScan);
        return size;
    }
    return 0;
}

template<>
int64_t AllShortestPathMorsel<true>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) {
    throw common::NotImplementedException("Not implemented for TRACK_PATH and nTkS scheduler. ");
}

} // namespace processor
} // namespace kuzu
