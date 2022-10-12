#include "src/processor/include/physical_plan/operator/unwind.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Unwind::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    expressionEvaluator->init(*resultSet, context->memoryManager);
    valueVector = make_shared<ValueVector>(*exprReturnDataType->childType, context->memoryManager);
    resultSet->dataChunks[outDataPos.dataChunkPos]->insert(outDataPos.valueVectorPos, valueVector);
    return resultSet;
}

bool Unwind::getNextTuples() {
    metrics->executionTime.start();
    if (startIndex > 0 && inputList->size > startIndex) {
        auto totalElementsCopy = min(DEFAULT_VECTOR_CAPACITY, inputList->size - startIndex);
        auto outputValueVector =
            resultSet->dataChunks[outDataPos.dataChunkPos]->valueVectors[outDataPos.valueVectorPos];
        auto numOfBytes = Types::getDataTypeSize(exprReturnDataType->childType->typeID);
        for (auto pos = startIndex; pos < (totalElementsCopy + startIndex); pos++) {
            ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(*outputValueVector, pos - startIndex,
                reinterpret_cast<uint8_t*>(inputList->overflowPtr) + pos * numOfBytes);
        }
        startIndex += totalElementsCopy;
        outputValueVector->state->initOriginalAndSelectedSize(totalElementsCopy);
    } else if (startIndex == 0 || startIndex >= inputList->size) {
        if (!children[0]->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        expressionEvaluator->evaluate();
        inputList = (gf_list_t*)expressionEvaluator->resultVector->values;
        auto totalElementsCopy = min(DEFAULT_VECTOR_CAPACITY, inputList->size);
        auto outputValueVector =
            resultSet->dataChunks[outDataPos.dataChunkPos]->valueVectors[outDataPos.valueVectorPos];
        auto numOfBytes = Types::getDataTypeSize(exprReturnDataType->childType->typeID);
        for (startIndex = 0; startIndex < totalElementsCopy; startIndex++) {
            ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(*outputValueVector, startIndex,
                reinterpret_cast<uint8_t*>(inputList->overflowPtr) + startIndex * numOfBytes);
        }
        outputValueVector->state->initOriginalAndSelectedSize(totalElementsCopy);
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
