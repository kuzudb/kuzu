#include "processor/operator/recursive_extend/variable_length_state.h"

namespace kuzu {
namespace processor {

#if defined(__GNUC__) || defined(__GNUG__)
template<>
void VariableLengthMorsel<false>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) {
    for (auto i = 0u; i < tmpDstNodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = tmpDstNodeIDVector->state->selVector->selectedPositions[i];
        auto nodeID = tmpDstNodeIDVector->getValue<common::nodeID_t>(pos);
        auto state = bfsSharedState->visitedNodes[nodeID.offset];
        if (state == NOT_VISITED_DST || state == VISITED_DST) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_DST_NEW);
        } else if (state == NOT_VISITED || state == VISITED) {
            __sync_bool_compare_and_swap(
                &bfsSharedState->visitedNodes[nodeID.offset], state, VISITED_NEW);
        }
        auto member = bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset];
        if (!member) {
            auto entry = std::make_shared<multiplicityAndLevel>(
                boundNodeMultiplicity, bfsSharedState->currentLevel + 1, nullptr);
            if (atomic_compare_exchange_strong(
                    &bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset], &member, entry)) {
                // no need to do anything, current thread was successful
            } else {
                member = bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset];
                member->multiplicity.fetch_add(boundNodeMultiplicity);
            }
        } else {
            if (member->bfsLevel != bfsSharedState->currentLevel + 1) {
                auto entry = std::make_shared<multiplicityAndLevel>(
                    boundNodeMultiplicity, bfsSharedState->currentLevel + 1, member);
                if (atomic_compare_exchange_strong(
                        &bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset], &member,
                        entry)) {
                    // no need to do anything, current thread was successful
                } else {
                    member = bfsSharedState->nodeIDMultiplicityToLevel[nodeID.offset];
                    member->multiplicity.fetch_add(boundNodeMultiplicity);
                }
            } else {
                member->multiplicity.fetch_add(boundNodeMultiplicity);
            }
        }
    }
}
#endif

#if defined(__GNUC__) || defined(__GNUG__)
template<>
void VariableLengthMorsel<true>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) {
    throw common::NotImplementedException("Not implemented for TRACK_PATH and nTkS scheduler. ");
}
#endif

template<>
int64_t VariableLengthMorsel<false>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) {
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    bool exitOuterLoop = false;
    while (startScanIdxAndSize.first < endIdx && size < common::DEFAULT_VECTOR_CAPACITY) {
        if (bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST ||
            bfsSharedState->visitedNodes[startScanIdxAndSize.first] == VISITED_DST_NEW) {
            auto entry = bfsSharedState->nodeIDMultiplicityToLevel[startScanIdxAndSize.first];
            while (entry && entry->bfsLevel >= lowerBound) {
                auto multiplicity = entry->multiplicity.load(std::memory_order_relaxed);
                do {
                    dstNodeIDVector->setValue<common::nodeID_t>(
                        size, common::nodeID_t{startScanIdxAndSize.first, tableID});
                    pathLengthVector->setValue<int64_t>(size, entry->bfsLevel);
                    size++;
                } while (--multiplicity && size < common::DEFAULT_VECTOR_CAPACITY);
                /// ValueVector capacity was reached, keep morsel state saved & exit from loop.
                if (multiplicity > 0) {
                    entry->multiplicity.store(multiplicity, std::memory_order_relaxed);
                    bfsSharedState->nodeIDMultiplicityToLevel[startScanIdxAndSize.first] = entry;
                    exitOuterLoop = true;
                    break;
                }
                if (size == common::DEFAULT_VECTOR_CAPACITY) {
                    bfsSharedState->nodeIDMultiplicityToLevel[startScanIdxAndSize.first] =
                        entry->next;
                    exitOuterLoop = true;
                    break;
                }
                entry = entry->next;
            }
        }
        if (!exitOuterLoop) {
            startScanIdxAndSize.first++;
        }
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
int64_t VariableLengthMorsel<true>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) {
    throw common::NotImplementedException("Not implemented for TRACK_PATH and nTkS scheduler. ");
}

} // namespace processor
} // namespace kuzu
