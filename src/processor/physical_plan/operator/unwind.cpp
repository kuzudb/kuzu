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
    if (isExprEvaluated) {
        metrics->executionTime.stop();
        return false;
    }
    expressionEvaluator->evaluate();
    auto inputList = (gf_list_t*)expressionEvaluator->resultVector->values;
    isExprEvaluated = true;
    uint32_t numOfBytes = Types::getDataTypeSize(expression->getDataType().childType->typeID);
    memcpy(valueVector->values, reinterpret_cast<uint8_t*>(inputList->overflowPtr),
        numOfBytes * inputList->size);
    valueVector->state->initOriginalAndSelectedSize(inputList->size);
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
