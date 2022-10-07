#include "src/processor/include/physical_plan/operator/unwind/unwind.h"

#include "src/binder/expression/include/literal_expression.h"
#include "src/common/include/overflow_buffer_utils.h"
#include "src/expression_evaluator/include/function_evaluator.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Unwind::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    expressionEvaluator->init(*resultSet, context->memoryManager);
    valueVector =
        make_shared<ValueVector>(*expression->getDataType().childType, context->memoryManager);
    outDataChunk->insert(outDataPos.valueVectorPos, valueVector);
    return resultSet;
}

bool Unwind::getNextTuples() {
    metrics->executionTime.start();
    if (isExprEvaluated) {
        metrics->executionTime.stop();
        return false;
    }
    expressionEvaluator->evaluate();
    inputList = (gf_list_t*)expressionEvaluator->resultVector->values;
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
