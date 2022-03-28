#include "include/reference_evaluator.h"

namespace graphflow {
namespace evaluator {

inline static bool isTrue(ValueVector& vector, uint64_t pos) {
    assert(vector.dataType == BOOL);
    return !vector.isNull(pos) && ((bool*)vector.values.get())[pos];
}

void ReferenceExpressionEvaluator::init(const ResultSet& resultSet, MemoryManager* memoryManager) {
    assert(children.empty());
    resultVector =
        resultSet.dataChunks[vectorPos.dataChunkPos]->valueVectors[vectorPos.valueVectorPos];
}

uint64_t ReferenceExpressionEvaluator::select(sel_t* selectedPos) {
    uint64_t numSelectedValues = 0;
    if (resultVector->state->isFlat()) {
        auto pos = resultVector->state->getPositionOfCurrIdx();
        numSelectedValues += isTrue(*resultVector, pos);
    } else {
        if (resultVector->state->isUnfiltered()) {
            for (auto i = 0u; i < resultVector->state->selectedSize; i++) {
                selectedPos[numSelectedValues] = i;
                numSelectedValues += isTrue(*resultVector, i);
            }
        } else {
            for (auto i = 0u; i < resultVector->state->selectedSize; i++) {
                auto pos = resultVector->state->selectedPositions[i];
                selectedPos[numSelectedValues] = pos;
                numSelectedValues += isTrue(*resultVector, pos);
            }
        }
    }
    return numSelectedValues;
}

} // namespace evaluator
} // namespace graphflow
