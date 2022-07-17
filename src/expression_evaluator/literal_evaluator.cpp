#include "include/literal_evaluator.h"

#include "src/common/include/vector/value_vector_utils.h"

namespace graphflow {
namespace evaluator {

void LiteralExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    assert(children.empty());
    resultVector = make_shared<ValueVector>(memoryManager, literal->dataType);
    ValueVectorUtils::addLiteralToStructuredVector(*resultVector, 0, *literal);
    resultVector->state = DataChunkState::getSingleValueDataChunkState();
}

bool LiteralExpressionEvaluator::select(SelectionVector& selVector) {
    auto pos = resultVector->state->getPositionOfCurrIdx();
    assert(pos == 0u);
    return resultVector->values[pos] == true && (!resultVector->isNull(pos));
}

} // namespace evaluator
} // namespace graphflow
