#include "expression_evaluator/expression_evaluator_utils.h"

#include "common/types/value/value.h"
#include "processor/expression_mapper.h"

using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace evaluator {

Value ExpressionEvaluatorUtils::evaluateConstantExpression(
    const std::shared_ptr<binder::Expression>& expression, storage::MemoryManager* memoryManager) {
    auto evaluator = ExpressionMapper::getConstantEvaluator(expression);
    auto emptyResultSet = std::make_unique<ResultSet>(0);
    evaluator->init(*emptyResultSet, memoryManager);
    evaluator->evaluate(nullptr);
    auto& selVector = evaluator->resultVector->state->getSelVector();
    KU_ASSERT(selVector.getSelSize() == 1);
    return *evaluator->resultVector->getAsValue(selVector[0]);
}

} // namespace evaluator
} // namespace kuzu
