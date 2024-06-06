#include "expression_evaluator/literal_evaluator.h"

#include "common/types/value/value.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::main;

namespace kuzu {
namespace evaluator {

void LiteralExpressionEvaluator::evaluate(EvaluateData& evaluateData) {
    auto cnt = evaluateData.count;
    if (cnt == 1) {
        return;
    }
    for (auto i = 1ul; i < cnt; i++) {
        resultVector->copyFromVectorData(i, resultVector.get(), 0);
    }
    resultVector->setState(std::make_shared<DataChunkState>(cnt));
    resultVector->state->getSelVectorUnsafe().setSelSize(cnt);
}

bool LiteralExpressionEvaluator::select(SelectionVector&, EvaluateData&) {
    KU_ASSERT(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    auto pos = resultVector->state->getSelVector()[0];
    KU_ASSERT(pos == 0u);
    return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

void LiteralExpressionEvaluator::resolveResultVector(const processor::ResultSet& /*resultSet*/,
    MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(*value.getDataType(), memoryManager);
    resultVector->setState(DataChunkState::getSingleValueDataChunkState());
    if (value.isNull()) {
        resultVector->setNull(0 /* pos */, true);
    } else {
        resultVector->copyFromValue(resultVector->state->getSelVector()[0], value);
    }
}

} // namespace evaluator
} // namespace kuzu
