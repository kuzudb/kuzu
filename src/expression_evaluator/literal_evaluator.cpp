#include "expression_evaluator/literal_evaluator.h"

#include <memory>

#include "common/assert.h"
#include "common/data_chunk/data_chunk_state.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "processor/result/result_set.h"
#include "storage/buffer_manager/memory_manager.h"

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
