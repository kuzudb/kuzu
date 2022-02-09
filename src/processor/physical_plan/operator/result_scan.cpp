#include "src/processor/include/physical_plan/operator/result_scan.h"

#include <cstring>

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> ResultScan::initResultSet() {
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[outDataChunkPos];
    for (auto i = 0u; i < inDataPoses.size(); ++i) {
        auto [inDataChunkPos, inValueVectorPos] = inDataPoses[i];
        auto& inValueVector =
            *resultSetToCopyFrom->dataChunks[inDataChunkPos]->valueVectors[inValueVectorPos];
        auto outValueVector =
            make_shared<ValueVector>(context.memoryManager, inValueVector.dataType);
        outDataChunk->insert(outValueVectorsPos[i], outValueVector);
    }
    return resultSet;
}

void ResultScan::reInitToRerunSubPlan() {
    SourceOperator::reInitToRerunSubPlan(*resultSet);
    isFirstExecution = true;
}

/**
 * ResultScan assumes all input is flat. Thus, getNextTuples() should be called exactly twice. On
 * the first call ResultScan copies in the flat input tuple. On the second call ResultScan
 * terminates the execution.
 *
 * NOTE: resultScan's dateChunk state always have size = 1 and currentIdx = 0. So in the following
 * code we directly write on currentIdx = 0.
 */
bool ResultScan::getNextTuples() {
    metrics->executionTime.start();
    if (isFirstExecution) {
        isFirstExecution = false;
        for (auto i = 0u; i < inDataPoses.size(); ++i) {
            auto [inDataChunkPos, inValueVectorPos] = inDataPoses[i];
            auto& inValueVector =
                *this->resultSetToCopyFrom->dataChunks[inDataChunkPos]->getValueVector(
                    inValueVectorPos);
            assert(inValueVector.state->isFlat());
            auto outValueVector = outDataChunk->valueVectors[outValueVectorsPos[i]];
            auto pos = inValueVector.state->getPositionOfCurrIdx();
            if (inValueVector.isNull(pos)) {
                outValueVector->setNull(0, true /* isNull */);
                continue;
            }
            auto elementSize = TypeUtils::getDataTypeSize(inValueVector.dataType);
            memcpy(
                &outValueVector->values[0], inValueVector.values + pos * elementSize, elementSize);
            outValueVector->setNull(0, false /* isNull */);
        }
        outDataChunk->state->initOriginalAndSelectedSize(1);
        outDataChunk->state->currIdx = 0;
        metrics->executionTime.stop();
        metrics->numOutputTuple.incrementByOne();
        return true;
    } else {
        metrics->executionTime.stop();
        return false;
    }
}

} // namespace processor
} // namespace graphflow
