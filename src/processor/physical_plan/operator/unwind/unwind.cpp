#include "src/processor/include/physical_plan/operator/unwind/unwind.h"

#include "src/binder/expression/include/literal_expression.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> Unwind::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    outDataChunk->state = DataChunkState::getSingleValueDataChunkState();
    outValueVector =
        make_shared<ValueVector>(expression->getChild(0)->dataType, context->memoryManager);
    outValueVector->setSequential();
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    return resultSet;
}

bool Unwind::getNextTuples() {
    metrics->executionTime.start();
    if (pos == expression->getNumChildren()) {
        metrics->executionTime.stop();
        return false; // condition = until all children have finished sending their literals
    }
    // outValueVector->state->isFlat();
    auto childExpr = reinterpret_cast<LiteralExpression*>(expression->getChild(pos).get());
    Literal literal = *(childExpr->literal);
    ValueVectorUtils::addLiteralToStructuredVector(
        *outValueVector, outValueVector->state->getPositionOfCurrIdx(), literal);
    pos++;
    return true;
}

} // namespace processor
} // namespace graphflow
