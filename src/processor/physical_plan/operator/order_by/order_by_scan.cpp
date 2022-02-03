#include "src/processor/include/physical_plan/operator/order_by/order_by_scan.h"

namespace graphflow {
namespace processor {

pair<uint64_t, uint64_t> OrderByScan::getNextFactorizedTableIdxAndTupleIdxPair() {
    auto tupleInfoBuffer =
        sharedState->sortedKeyBlocks->front()->getMemBlockData() +
        (nextTupleIdxToReadInMemBlock + 1) * sharedState->keyBlockEntrySizeInBytes -
        sizeof(uint64_t);
    uint16_t factorizedTableIdx = OrderByKeyEncoder::getEncodedFactorizedTableIdx(tupleInfoBuffer);
    uint64_t tupleIdx = OrderByKeyEncoder::getEncodedTupleIdx(tupleInfoBuffer);
    return make_pair(factorizedTableIdx, tupleIdx);
}

shared_ptr<ResultSet> OrderByScan::initResultSet() {
    resultSet = populateResultSet();
    for (auto i = 0u; i < outDataPoses.size(); i++) {
        auto outDataPos = outDataPoses[i];
        auto outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
        outDataChunk->insert(outDataPos.valueVectorPos,
            make_shared<ValueVector>(context.memoryManager,
                sharedState->factorizedTables[0]->getTupleSchema().columns[i].dataType));
        columnsToReadInFactorizedTable.emplace_back(i);
    }
    scanSingleTuple =
        sharedState->factorizedTables[0]->hasUnflatColToRead(columnsToReadInFactorizedTable);
    return resultSet;
}

bool OrderByScan::getNextTuples() {
    metrics->executionTime.start();
    auto numTuplesInMemBlock = sharedState->sortedKeyBlocks->front()->numEntriesInMemBlock;
    // If there is no more tuples to read, just return false.
    if (nextTupleIdxToReadInMemBlock >= numTuplesInMemBlock) {
        metrics->executionTime.stop();
        return false;
    } else {
        // If there is an unflat column in columnsToReadInFactorizedTable, we can only read one
        // tuple at a time. Otherwise, we can read min(DEFAULT_VECTOR_CAPACITY,
        // numTuplesRemainingInMemBlock) tuples.
        if (scanSingleTuple) {
            auto factorizedTableIdxAndTupleIdxPair = getNextFactorizedTableIdxAndTupleIdxPair();
            sharedState->factorizedTables[factorizedTableIdxAndTupleIdxPair.first]->scan(
                columnsToReadInFactorizedTable, outDataPoses, *resultSet,
                factorizedTableIdxAndTupleIdxPair.second, 1 /* numTuples */);
            nextTupleIdxToReadInMemBlock++;
            metrics->numOutputTuple.increase(1);
        } else {
            auto numTuplesToRead =
                min(DEFAULT_VECTOR_CAPACITY, numTuplesInMemBlock - nextTupleIdxToReadInMemBlock);
            for (auto i = 0u; i < numTuplesToRead; i++) {
                auto factorizedTableIdxAndTupleIdxPair = getNextFactorizedTableIdxAndTupleIdxPair();
                // todo: add support to read values from factorizedTable to
                // unflat valueVectors. issue: 443
                sharedState->factorizedTables[factorizedTableIdxAndTupleIdxPair.first]
                    ->readFlatTupleToUnflatVector(columnsToReadInFactorizedTable, outDataPoses,
                        *resultSet, factorizedTableIdxAndTupleIdxPair.second, i);
                nextTupleIdxToReadInMemBlock++;
                metrics->numOutputTuple.increase(1);
            }
        }

        metrics->executionTime.stop();
        return true;
    }
}

} // namespace processor
} // namespace graphflow
