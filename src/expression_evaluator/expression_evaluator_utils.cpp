#include "expression_evaluator/expression_evaluator_utils.h"

#include "common/types/value/value.h"
#include "processor/expression_mapper.h"

using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace evaluator {

std::unique_ptr<Value> ExpressionEvaluatorUtils::evaluateConstantExpression(
    const std::shared_ptr<binder::Expression>& expression, storage::MemoryManager* memoryManager) {
    auto evaluator = ExpressionMapper::getConstantEvaluator(expression);
    auto emptyResultSet = std::make_unique<ResultSet>(0);
    evaluator->init(*emptyResultSet, memoryManager);
    evaluator->evaluate();
    auto selVector = evaluator->resultVector->state->selVector.get();
    KU_ASSERT(selVector->selectedSize == 1);
    return evaluator->resultVector->getAsValue(selVector->selectedPositions[0]);
}

} // namespace evaluator
} // namespace kuzu
