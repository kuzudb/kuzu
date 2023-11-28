#include "expression_evaluator/expression_evaluator.h"

#include <vector>

#include "common/data_chunk/data_chunk_state.h"
#include "processor/result/result_set.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace evaluator {

void ExpressionEvaluator::init(
    const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) {
    for (auto& child : children) {
        child->init(resultSet, memoryManager);
    }
    resolveResultVector(resultSet, memoryManager);
}

void ExpressionEvaluator::resolveResultStateFromChildren(
    const std::vector<ExpressionEvaluator*>& inputEvaluators) {
    if (resultVector->state != nullptr) {
        return;
    }
    for (auto& input : inputEvaluators) {
        if (!input->isResultFlat()) {
            isResultFlat_ = false;
            resultVector->setState(input->resultVector->state);
            return;
        }
    }
    // All children are flat.
    isResultFlat_ = true;
    resultVector->setState(DataChunkState::getSingleValueDataChunkState());
}

} // namespace evaluator
} // namespace kuzu
