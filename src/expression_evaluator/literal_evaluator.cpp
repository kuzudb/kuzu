#include "expression_evaluator/literal_evaluator.h"

#include "common/types/value/value.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::main;

namespace kuzu {
namespace evaluator {

void LiteralExpressionEvaluator::evaluate() {
    auto cnt = localState.count;
    for (auto i = 1ul; i < cnt; i++) {
        resultVector->copyFromVectorData(i, resultVector.get(), 0);
    }
    if (cnt > 1) {
        unflatState->getSelVectorUnsafe().setSelSize(cnt);
        resultVector->state = unflatState;
    }
}

bool LiteralExpressionEvaluator::select(SelectionVector&) {
    KU_ASSERT(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    auto pos = resultVector->state->getSelVector()[0];
    KU_ASSERT(pos == 0u);
    return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

void LiteralExpressionEvaluator::resolveResultVector(const processor::ResultSet& /*resultSet*/,
    MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(value.getDataType().copy(), memoryManager);
    resultVector->setState(DataChunkState::getSingleValueDataChunkState());
    unflatState = std::make_shared<DataChunkState>();
    if (value.isNull()) {
        resultVector->setNull(0 /* pos */, true);
    } else {
        resultVector->copyFromValue(resultVector->state->getSelVector()[0], value);
    }
}

} // namespace evaluator
} // namespace kuzu
