#include "processor/operator/order_by/order_by_scan.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> OrderByScan::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    for (auto i = 0u; i < outDataPoses.size(); i++) {
        auto outDataPos = outDataPoses[i];
        auto outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
        auto valueVector =
            make_shared<ValueVector>(sharedState->getDataType(i), context->memoryManager);
        outDataChunk->insert(outDataPos.valueVectorPos, valueVector);
        vectorsToRead.emplace_back(valueVector);
    }
    initMergedKeyBlockScanStateIfNecessary();
    return resultSet;
}

bool OrderByScan::getNextTuples() {
    metrics->executionTime.start();
    // If there is no more tuples to read, just return false.
    if (mergedKeyBlockScanState == nullptr ||
        mergedKeyBlockScanState->nextTupleIdxToReadInMergedKeyBlock >=
            mergedKeyBlockScanState->mergedKeyBlock->getNumTuples()) {
        metrics->executionTime.stop();
        return false;
    } else {
        // If there is an unflat col in factorizedTable, we can only read one
        // tuple at a time. Otherwise, we can read min(DEFAULT_VECTOR_CAPACITY,
        // numTuplesRemainingInMemBlock) tuples.
        if (mergedKeyBlockScanState->scanSingleTuple) {
            auto tupleInfoBuffer = mergedKeyBlockScanState->blockPtrInfo->curTuplePtr +
                                   mergedKeyBlockScanState->tupleIdxAndFactorizedTableIdxOffset;
            auto blockIdx = OrderByKeyEncoder::getEncodedFTBlockIdx(tupleInfoBuffer);
            auto blockOffset = OrderByKeyEncoder::getEncodedFTBlockOffset(tupleInfoBuffer);
            auto ft =
                sharedState->factorizedTables[OrderByKeyEncoder::getEncodedFTIdx(tupleInfoBuffer)];
            ft->scan(vectorsToRead, blockIdx * ft->getNumTuplesPerBlock() + blockOffset,
                1 /* numTuples */);
            mergedKeyBlockScanState->blockPtrInfo->curTuplePtr +=
                mergedKeyBlockScanState->mergedKeyBlock->getNumBytesPerTuple();
            mergedKeyBlockScanState->blockPtrInfo->updateTuplePtrIfNecessary();
            mergedKeyBlockScanState->nextTupleIdxToReadInMergedKeyBlock++;
            metrics->numOutputTuple.increase(1);
        } else {
            auto numTuplesToRead = min(DEFAULT_VECTOR_CAPACITY,
                mergedKeyBlockScanState->mergedKeyBlock->getNumTuples() -
                    mergedKeyBlockScanState->nextTupleIdxToReadInMergedKeyBlock);
            auto numTuplesRead = 0;
            while (numTuplesRead < numTuplesToRead) {
                auto numTuplesToReadInCurBlock = min(numTuplesToRead - numTuplesRead,
                    mergedKeyBlockScanState->blockPtrInfo->getNumTuplesLeftInCurBlock());

                for (auto i = 0u; i < numTuplesToReadInCurBlock; i++) {
                    auto tupleInfoBuffer =
                        mergedKeyBlockScanState->blockPtrInfo->curTuplePtr +
                        mergedKeyBlockScanState->tupleIdxAndFactorizedTableIdxOffset;
                    auto blockIdx = OrderByKeyEncoder::getEncodedFTBlockIdx(tupleInfoBuffer);
                    auto blockOffset = OrderByKeyEncoder::getEncodedFTBlockOffset(tupleInfoBuffer);
                    auto ft =
                        sharedState
                            ->factorizedTables[OrderByKeyEncoder::getEncodedFTIdx(tupleInfoBuffer)];
                    mergedKeyBlockScanState->tuplesToRead[numTuplesRead + i] =
                        ft->getTuple(blockIdx * ft->getNumTuplesPerBlock() + blockOffset);
                    mergedKeyBlockScanState->blockPtrInfo->curTuplePtr +=
                        mergedKeyBlockScanState->mergedKeyBlock->getNumBytesPerTuple();
                }
                mergedKeyBlockScanState->blockPtrInfo->updateTuplePtrIfNecessary();
                numTuplesRead += numTuplesToReadInCurBlock;
            }
            // TODO(Ziyi): This is a hacky way of using factorizedTable::lookup function,
            // since the tuples in tuplesToRead may not belong to factorizedTable0. The
            // lookup function doesn't perform a check on whether it holds all the tuples in
            // tuplesToRead. We should optimize this lookup function in the orderByScan
            // optimization PR.
            sharedState->factorizedTables[0]->lookup(vectorsToRead,
                mergedKeyBlockScanState->colsToScan, mergedKeyBlockScanState->tuplesToRead.get(), 0,
                numTuplesToRead);
            metrics->numOutputTuple.increase(numTuplesToRead);
            mergedKeyBlockScanState->nextTupleIdxToReadInMergedKeyBlock += numTuplesToRead;
        }
        metrics->executionTime.stop();
        return true;
    }
}

void OrderByScan::initMergedKeyBlockScanStateIfNecessary() {
    if (sharedState->sortedKeyBlocks->empty()) {
        return;
    }
    mergedKeyBlockScanState = make_unique<MergedKeyBlockScanState>();
    mergedKeyBlockScanState->nextTupleIdxToReadInMergedKeyBlock = 0;
    mergedKeyBlockScanState->mergedKeyBlock = sharedState->sortedKeyBlocks->front();
    mergedKeyBlockScanState->tupleIdxAndFactorizedTableIdxOffset =
        mergedKeyBlockScanState->mergedKeyBlock->getNumBytesPerTuple() - 8;
    mergedKeyBlockScanState->colsToScan = vector<uint32_t>(vectorsToRead.size());
    iota(mergedKeyBlockScanState->colsToScan.begin(), mergedKeyBlockScanState->colsToScan.end(), 0);
    mergedKeyBlockScanState->scanSingleTuple = sharedState->factorizedTables[0]->hasUnflatCol();
    if (!mergedKeyBlockScanState->scanSingleTuple) {
        mergedKeyBlockScanState->tuplesToRead = make_unique<uint8_t*[]>(DEFAULT_VECTOR_CAPACITY);
    }
    mergedKeyBlockScanState->blockPtrInfo = make_unique<BlockPtrInfo>(0 /* startTupleIdx */,
        mergedKeyBlockScanState->mergedKeyBlock->getNumTuples(),
        mergedKeyBlockScanState->mergedKeyBlock);
}

} // namespace processor
} // namespace kuzu
