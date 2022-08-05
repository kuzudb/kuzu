#include "src/processor/include/physical_plan/operator/order_by/order_by_scan.h"

namespace graphflow {
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
    scanSingleTuple = sharedState->factorizedTables[0]->hasUnflatCol();

    mergedKeyBlock = sharedState->sortedKeyBlocks->front();
    tupleIdxAndFactorizedTableIdxOffset = mergedKeyBlock->getNumBytesPerTuple() - 8;
    colsToScan = vector<uint32_t>(vectorsToRead.size());
    iota(colsToScan.begin(), colsToScan.end(), 0);
    if (!scanSingleTuple) {
        tuplesToRead = make_unique<uint8_t*[]>(DEFAULT_VECTOR_CAPACITY);
    }
    blockPtrInfo = make_unique<BlockPtrInfo>(
        0 /* startTupleIdx */, mergedKeyBlock->getNumTuples(), mergedKeyBlock);
    return resultSet;
}

bool OrderByScan::getNextTuples() {
    metrics->executionTime.start();
    // If there is no more tuples to read, just return false.
    if (nextTupleIdxToReadInMergedKeyBlock >= mergedKeyBlock->getNumTuples()) {
        metrics->executionTime.stop();
        return false;
    } else {
        // If there is an unflat col in factorizedTable, we can only read one
        // tuple at a time. Otherwise, we can read min(DEFAULT_VECTOR_CAPACITY,
        // numTuplesRemainingInMemBlock) tuples.
        if (scanSingleTuple) {
            auto tupleInfoBuffer = blockPtrInfo->curTuplePtr + tupleIdxAndFactorizedTableIdxOffset;
            auto blockIdx = OrderByKeyEncoder::getEncodedFTBlockIdx(tupleInfoBuffer);
            auto blockOffset = OrderByKeyEncoder::getEncodedFTBlockOffset(tupleInfoBuffer);
            auto ft =
                sharedState->factorizedTables[OrderByKeyEncoder::getEncodedFTIdx(tupleInfoBuffer)];
            ft->scan(vectorsToRead, blockIdx * ft->getNumTuplesPerBlock() + blockOffset,
                1 /* numTuples */);
            blockPtrInfo->curTuplePtr += mergedKeyBlock->getNumBytesPerTuple();
            blockPtrInfo->updateTuplePtrIfNecessary();
            nextTupleIdxToReadInMergedKeyBlock++;
            metrics->numOutputTuple.increase(1);
        } else {
            auto numTuplesToRead = min(DEFAULT_VECTOR_CAPACITY,
                mergedKeyBlock->getNumTuples() - nextTupleIdxToReadInMergedKeyBlock);
            auto numTuplesRead = 0;
            while (numTuplesRead < numTuplesToRead) {
                auto numTuplesToReadInCurBlock = min(
                    numTuplesToRead - numTuplesRead, blockPtrInfo->getNumTuplesLeftInCurBlock());

                for (auto i = 0u; i < numTuplesToReadInCurBlock; i++) {
                    auto tupleInfoBuffer =
                        blockPtrInfo->curTuplePtr + tupleIdxAndFactorizedTableIdxOffset;
                    auto blockIdx = OrderByKeyEncoder::getEncodedFTBlockIdx(tupleInfoBuffer);
                    auto blockOffset = OrderByKeyEncoder::getEncodedFTBlockOffset(tupleInfoBuffer);
                    auto ft =
                        sharedState
                            ->factorizedTables[OrderByKeyEncoder::getEncodedFTIdx(tupleInfoBuffer)];
                    tuplesToRead[numTuplesRead + i] =
                        ft->getTuple(blockIdx * ft->getNumTuplesPerBlock() + blockOffset);
                    blockPtrInfo->curTuplePtr += mergedKeyBlock->getNumBytesPerTuple();
                }
                blockPtrInfo->updateTuplePtrIfNecessary();
                numTuplesRead += numTuplesToReadInCurBlock;
            }
            // TODO(Ziyi): This is a hacky way of using factorizedTable::lookup function,
            // since the tuples in tuplesToRead may not belong to factorizedTable0. The
            // lookup function doesn't perform a check on whether it holds all the tuples in
            // tuplesToRead. We should optimize this lookup function in the orderByScan
            // optimization PR.
            sharedState->factorizedTables[0]->lookup(
                vectorsToRead, colsToScan, tuplesToRead.get(), 0, numTuplesToRead);
            metrics->numOutputTuple.increase(numTuplesToRead);
            nextTupleIdxToReadInMergedKeyBlock += numTuplesToRead;
        }
        metrics->executionTime.stop();
        return true;
    }
}

} // namespace processor
} // namespace graphflow
