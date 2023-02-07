#include "expression_evaluator/literal_evaluator.h"

#include "common/vector/value_vector_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

bool LiteralExpressionEvaluator::select(SelectionVector& selVector) {
    assert(resultVector->dataType.typeID == BOOL);
    auto pos = resultVector->state->selVector->selectedPositions[0];
    assert(pos == 0u);
    return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

void LiteralExpressionEvaluator::resolveResultVector(
    const processor::ResultSet& resultSet, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(value->getDataType(), memoryManager);
    resultVector->addValue(0, *value);
    resultVector->state = DataChunkState::getSingleValueDataChunkState();
}

} // namespace evaluator
} // namespace kuzu
