#include "src/processor/include/physical_plan/operator/unwind/unwind.h"

#include "src/binder/expression/include/literal_expression.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Unwind::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    outDataChunk->state = DataChunkState::getSingleValueDataChunkState();
    expressionEvaluator->init(*resultSet, context->memoryManager);
    outDataChunk->insert(outDataPos.valueVectorPos, expressionEvaluator->resultVector);
    return resultSet;
}

bool Unwind::getNextTuples() {
    metrics->executionTime.start();
    if (expr_evaluated) {
        metrics->executionTime.stop();
        return false;
    }
    expressionEvaluator->evaluate();
    expr_evaluated = true;
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
