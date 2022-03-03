#include "src/processor/include/physical_plan/operator/order_by/order_by_scan.h"

namespace graphflow {
namespace processor {

pair<uint64_t, uint64_t> OrderByScan::getNextFactorizedTableIdxAndTupleIdxPair() {
    auto tupleInfoBuffer =
        sharedState->sortedKeyBlocks->front()->getTuple(nextTupleIdxToReadInMemBlock + 1) -
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
        auto valueVector =
            make_shared<ValueVector>(context.memoryManager, sharedState->getDataType(i));
        outDataChunk->insert(outDataPos.valueVectorPos, valueVector);
        vectorsToRead.emplace_back(valueVector);
    }
    scanSingleTuple = sharedState->factorizedTables[0]->hasUnflatCol();
    return resultSet;
}

bool OrderByScan::getNextTuples() {
    metrics->executionTime.start();
    auto numTuplesInMemBlock = sharedState->sortedKeyBlocks->front()->getNumTuples();
    // If there is no more tuples to read, just return false.
    if (nextTupleIdxToReadInMemBlock >= numTuplesInMemBlock) {
        metrics->executionTime.stop();
        return false;
    } else {
        // If there is an unflat col in factorizedTable, we can only read one
        // tuple at a time. Otherwise, we can read min(DEFAULT_VECTOR_CAPACITY,
        // numTuplesRemainingInMemBlock) tuples.
        if (scanSingleTuple) {
            auto factorizedTableIdxAndTupleIdxPair = getNextFactorizedTableIdxAndTupleIdxPair();
            sharedState->factorizedTables[factorizedTableIdxAndTupleIdxPair.first]->scan(
                vectorsToRead, factorizedTableIdxAndTupleIdxPair.second, 1 /* numTuples */);
            nextTupleIdxToReadInMemBlock++;
            metrics->numOutputTuple.increase(1);
        } else {
            auto numTuplesToRead =
                min(DEFAULT_VECTOR_CAPACITY, numTuplesInMemBlock - nextTupleIdxToReadInMemBlock);
            for (auto i = 0u; i < numTuplesToRead; i++) {
                auto factorizedTableIdxAndTupleIdxPair = getNextFactorizedTableIdxAndTupleIdxPair();
                sharedState->factorizedTables[factorizedTableIdxAndTupleIdxPair.first]
                    ->scanTupleToVectorPos(
                        vectorsToRead, factorizedTableIdxAndTupleIdxPair.second, i);
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
