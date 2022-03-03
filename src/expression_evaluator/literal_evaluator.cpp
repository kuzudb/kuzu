#include "include/literal_evaluator.h"

namespace graphflow {
namespace evaluator {

void LiteralExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    assert(children.empty());
    if (castToUnstructured) {
        resultVector = make_shared<ValueVector>(memoryManager, UNSTRUCTURED);
        resultVector->addLiteralToUnstructuredVector(0, literal);
    } else {
        resultVector = make_shared<ValueVector>(memoryManager, literal.dataType);
        resultVector->addLiteralToStructuredVector(0, literal);
    }
    resultVector->state = DataChunkState::getSingleValueDataChunkState();
}

uint64_t LiteralExpressionEvaluator::select(sel_t* selectedPos) {
    auto pos = resultVector->state->getPositionOfCurrIdx();
    assert(pos == 0u);
    return resultVector->values[pos] == true && (!resultVector->isNull(pos));
}

} // namespace evaluator
} // namespace graphflow
