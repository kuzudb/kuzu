#include "expression_evaluator/literal_evaluator.h"

#include "common/vector/value_vector_utils.h"

namespace kuzu {
namespace evaluator {

void LiteralExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    assert(children.empty());
    resultVector = make_shared<ValueVector>(literal->dataType, memoryManager);
    ValueVectorUtils::addLiteralToStructuredVector(*resultVector, 0, *literal);
    resultVector->state = DataChunkState::getSingleValueDataChunkState();
}

bool LiteralExpressionEvaluator::select(SelectionVector& selVector) {
    assert(resultVector->dataType.typeID == BOOL);
    auto pos = resultVector->state->selVector->selectedPositions[0];
    assert(pos == 0u);
    return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

} // namespace evaluator
} // namespace kuzu
