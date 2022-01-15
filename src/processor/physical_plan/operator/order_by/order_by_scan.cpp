#include "src/processor/include/physical_plan/operator/order_by/order_by_scan.h"

namespace graphflow {
namespace processor {

pair<uint64_t, uint64_t> OrderByScan::getNextRowCollectionIdxAndRowIdxPair() {
    auto rowInfoBuffer = sharedState->sortedKeyBlocks->front()->getMemBlockData() +
                         (nextRowIdxToReadInMemBlock + 1) * sharedState->keyBlockEntrySizeInBytes -
                         sizeof(uint64_t);
    uint16_t rowCollectionIdx = OrderByKeyEncoder::getEncodedRowCollectionIdx(rowInfoBuffer);
    uint64_t rowIdx = OrderByKeyEncoder::getEncodedRowIdx(rowInfoBuffer);
    return make_pair(rowCollectionIdx, rowIdx);
}

shared_ptr<ResultSet> OrderByScan::initResultSet() {
    resultSet = populateResultSet();
    for (auto i = 0u; i < outDataPoses.size(); i++) {
        auto outDataPos = outDataPoses[i];
        auto outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
        outDataChunk->insert(outDataPos.valueVectorPos,
            make_shared<ValueVector>(context.memoryManager,
                sharedState->rowCollections[0]->getLayout().fields[i].dataType));
        fieldsToReadInRowCollection.emplace_back(i);
    }
    scanSingleTuple =
        sharedState->rowCollections[0]->hasOverflowColToRead(fieldsToReadInRowCollection);
    return resultSet;
}

bool OrderByScan::getNextTuples() {
    metrics->executionTime.start();
    auto numRowsInMemBlock = sharedState->sortedKeyBlocks->front()->numEntriesInMemBlock;
    // If there is no more rows to read, just return false.
    if (nextRowIdxToReadInMemBlock >= numRowsInMemBlock) {
        metrics->executionTime.stop();
        return false;
    } else {
        // If there is an overflow column in fieldsToReadInRowCollection, we can only read one
        // row at a time. Otherwise, we can read min(DEFAULT_VECTOR_CAPACITY,
        // numRowsRemainingInMemBlock) rows.
        if (scanSingleTuple) {
            auto rowCollectionIdxAndRowIdxPair = getNextRowCollectionIdxAndRowIdxPair();
            sharedState->rowCollections[rowCollectionIdxAndRowIdxPair.first]->scan(
                fieldsToReadInRowCollection, outDataPoses, *resultSet,
                rowCollectionIdxAndRowIdxPair.second, 1 /* numRowsToScan */);
            nextRowIdxToReadInMemBlock++;
            metrics->numOutputTuple.increase(1);
        } else {
            auto numRowsToLookup =
                min(DEFAULT_VECTOR_CAPACITY, numRowsInMemBlock - nextRowIdxToReadInMemBlock);
            for (auto i = 0u; i < numRowsToLookup; i++) {
                auto rowCollectionIdxAndRowIdxPair = getNextRowCollectionIdxAndRowIdxPair();
                // todo: add support to read values from rowCollection to
                // unflat valueVectors. issue: 443
                sharedState->rowCollections[rowCollectionIdxAndRowIdxPair.first]
                    ->readNonOverflowTupleToUnflatVector(fieldsToReadInRowCollection, outDataPoses,
                        *resultSet, rowCollectionIdxAndRowIdxPair.second, i);
                nextRowIdxToReadInMemBlock++;
                metrics->numOutputTuple.increase(1);
            }
        }

        metrics->executionTime.stop();
        return true;
    }
}

} // namespace processor
} // namespace graphflow
