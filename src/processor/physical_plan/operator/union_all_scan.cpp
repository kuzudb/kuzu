#include "src/processor/include/physical_plan/operator/union_all_scan.h"

namespace graphflow {
namespace processor {

unique_ptr<UnionAllScanMorsel> UnionAllScanSharedState::getMorsel() {
    lock_guard<mutex> lck{unionAllScanSharedStateMutex};
    // If we have read all the tuples for the current resultCollector, go to the next one.
    // Note that: a queryResult may contain 0 tuple.
    while (resultCollectorIdxToRead < unionAllChildren.size() &&
           nextTupleIdxToRead >= getFactorizedTable(resultCollectorIdxToRead)->getNumTuples()) {
        resultCollectorIdxToRead++;
        nextTupleIdxToRead = 0;
    }
    // We have read all the tuples in all query results, just return a nullptr.
    if (resultCollectorIdxToRead >= unionAllChildren.size()) {
        return nullptr;
    }

    auto numTuplesToRead = min(maxMorselSize,
        getFactorizedTable(resultCollectorIdxToRead)->getNumTuples() - nextTupleIdxToRead);
    auto unionAllScanMorsel = make_unique<UnionAllScanMorsel>(
        resultCollectorIdxToRead, nextTupleIdxToRead, numTuplesToRead);
    nextTupleIdxToRead += numTuplesToRead;
    return unionAllScanMorsel;
}

shared_ptr<ResultSet> UnionAllScan::initResultSet() {
    resultSet = populateResultSet();
    auto tupleSchema = unionAllScanSharedState->getTupleSchema();
    vector<uint64_t> fieldsToReadInFactorizedTable;
    for (auto i = 0u; i < outDataPoses.size(); i++) {
        auto outDataPos = outDataPoses[i];
        auto outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
        outDataChunk->insert(outDataPos.valueVectorPos,
            make_shared<ValueVector>(context.memoryManager, tupleSchema.fields[i].dataType));
        fieldsToReadInFactorizedTable.emplace_back(i);
    }
    unionAllScanSharedState->initForScanning();
    return resultSet;
}

bool UnionAllScan::getNextTuples() {
    metrics->executionTime.start();
    auto unionAllScanMorsel = unionAllScanSharedState->getMorsel();
    if (unionAllScanMorsel == nullptr) {
        return false;
    }

    // We scan one by one if there is an unflat column in the factorized tables (see how
    // maxMorselSize is set in initForScanning).
    unionAllScanSharedState->getFactorizedTable(unionAllScanMorsel->getChildIdx())
        ->scan(outDataPoses, *resultSet, unionAllScanMorsel->getStartTupleIdx(),
            unionAllScanMorsel->getNumTuples());
    metrics->numOutputTuple.increase(
        unionAllScanMorsel->getNumTuples() == 1 ?
            unionAllScanSharedState->getFactorizedTable(unionAllScanMorsel->getChildIdx())
                ->getNumFlatTuples(unionAllScanMorsel->getStartTupleIdx()) :
            unionAllScanMorsel->getNumTuples());
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
