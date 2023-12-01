#include "expression_evaluator/literal_evaluator.h"

#include "common/types/value/value.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

bool LiteralExpressionEvaluator::select(SelectionVector& /*selVector*/) {
    KU_ASSERT(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    auto pos = resultVector->state->selVector->selectedPositions[0];
    KU_ASSERT(pos == 0u);
    return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

void LiteralExpressionEvaluator::resolveResultVector(
    const processor::ResultSet& /*resultSet*/, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(*value->getDataType(), memoryManager);
    resultVector->setState(DataChunkState::getSingleValueDataChunkState());
    if (value->isNull()) {
        resultVector->setNull(0 /* pos */, true);
    } else {
        resultVector->copyFromValue(resultVector->state->selVector->selectedPositions[0], *value);
    }
}

} // namespace evaluator
} // namespace kuzu
