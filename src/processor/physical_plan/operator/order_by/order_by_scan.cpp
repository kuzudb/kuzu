#include "src/processor/include/physical_plan/operator/order_by/order_by_scan.h"

namespace graphflow {
namespace processor {

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
    return resultSet;
}

bool OrderByScan::getNextTuples() {
    metrics->executionTime.start();
    auto& resultKeyBlock = sharedState->sortedKeyBlocks->front();
    auto memBuffer = resultKeyBlock->getMemBlockData();
    // If there is no more rows to read, just return false.
    if (nextRowIdxToReadInMemBlock >= resultKeyBlock->numEntriesInMemBlock) {
        metrics->executionTime.stop();
        return false;
    } else {
        // The rowInfoBuffer saves rowCollectionID and rowID for each row.
        auto rowInfoBuffer =
            memBuffer + (nextRowIdxToReadInMemBlock + 1) * sharedState->keyBlockEntrySizeInBytes -
            sizeof(uint64_t);
        uint16_t rowCollectionIdx = OrderByKeyEncoder::getEncodedRowCollectionIdx(rowInfoBuffer);
        uint64_t rowIdx = OrderByKeyEncoder::getEncodedRowIdx(rowInfoBuffer);
        // todo: add support to read values from rowCollection to unflat valueVectors.
        // issue: 443
        sharedState->rowCollections[rowCollectionIdx]->scan(
            fieldsToReadInRowCollection, outDataPoses, *resultSet, rowIdx, 1);
        nextRowIdxToReadInMemBlock++;
        metrics->numOutputTuple.increase(1);
        metrics->executionTime.stop();
        return true;
    }
}

} // namespace processor
} // namespace graphflow
