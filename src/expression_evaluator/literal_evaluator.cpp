#include "expression_evaluator/literal_evaluator.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

bool LiteralExpressionEvaluator::select(SelectionVector& selVector) {
    assert(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    auto pos = resultVector->state->selVector->selectedPositions[0];
    assert(pos == 0u);
    return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

void LiteralExpressionEvaluator::resolveResultVector(
    const processor::ResultSet& resultSet, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(*value->getDataType(), memoryManager);
    if (value->isNull()) {
        resultVector->setNull(0 /* pos */, true);
    } else {
        ValueVector::copyValueToVector(resultVector->getData(), resultVector.get(), value.get());
    }
    resultVector->setState(DataChunkState::getSingleValueDataChunkState());
}

} // namespace evaluator
} // namespace kuzu
