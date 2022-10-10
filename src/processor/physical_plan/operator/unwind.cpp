#include "src/processor/include/physical_plan/operator/unwind.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Unwind::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    expressionEvaluator->init(*resultSet, context->memoryManager);
    valueVector =
        make_shared<ValueVector>(*expression->getDataType().childType, context->memoryManager);
    resultSet->dataChunks[outDataPos.dataChunkPos]->insert(outDataPos.valueVectorPos, valueVector);
    return resultSet;
}

bool Unwind::getNextTuples() {
    metrics->executionTime.start();
    if (!isExprEvaluated) {
        expressionEvaluator->evaluate();
        inputList = (gf_list_t*)expressionEvaluator->resultVector->values;
        isExprEvaluated = true;
    }
    if (startOffset >= inputList->size) {
        metrics->executionTime.stop();
        return false;
    }
    auto offsetLength =
        (startOffset + 2048 < inputList->size) ? 2048 : (inputList->size - startOffset);
    uint32_t numOfBytes = Types::getDataTypeSize(expression->getDataType().childType->typeID);
    memcpy(valueVector->values,
        reinterpret_cast<uint8_t*>(inputList->overflowPtr) + startOffset * numOfBytes,
        numOfBytes * offsetLength);
    valueVector->state->initOriginalAndSelectedSize(offsetLength);
    startOffset += 2048;
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
