#include "processor/operator/recursive_extend/variable_length_state.h"

namespace kuzu {
namespace processor {

// TODO: Populate this logic as per variable length recursive join
template<>
void VariableLengthMorsel<false>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) {}

template<>
void VariableLengthMorsel<true>::addToLocalNextBFSLevel(
    common::ValueVector* tmpDstNodeIDVector, uint64_t boundNodeMultiplicity) {
    throw common::NotImplementedException("Not implemented for TRACK_PATH and nTkS scheduler. ");
}

template<>
int64_t VariableLengthMorsel<false>::writeToVector(
    const std::shared_ptr<FactorizedTableScanSharedState>& inputFTableSharedState,
    std::vector<common::ValueVector*> vectorsToScan, std::vector<ft_col_idx_t> colIndicesToScan,
    common::ValueVector* dstNodeIDVector, common::ValueVector* pathLengthVector,
    common::table_id_t tableID, std::pair<uint64_t, int64_t> startScanIdxAndSize) {
    auto size = 0u;
    auto endIdx = startScanIdxAndSize.first + startScanIdxAndSize.second;
    while (startScanIdxAndSize.first < endIdx && size < common::DEFAULT_VECTOR_CAPACITY) {
        if (bfsSharedState->pathLength[startScanIdxAndSize.first] >= bfsSharedState->lowerBound) {
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
                prevDistMorselStartEndIdx = {startScanIdxAndSize.first, endIdx};
                bfsSharedState->nodeIDToMultiplicity[startScanIdxAndSize.first] = multiplicity;
                break;
            }
        }
        startScanIdxAndSize.first++;
    }
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
