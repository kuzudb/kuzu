#include "processor/operator/unwind.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Unwind::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    expressionEvaluator->init(*resultSet, context->memoryManager);
    outValueVector = resultSet->getValueVector(outDataPos);
}

bool Unwind::hasMoreToRead() const {
    return inputList.size > startIndex;
}

void Unwind::copyTuplesToOutVector(uint64_t startPos, uint64_t endPos) const {
    auto numOfBytes = Types::getDataTypeSize(outDataType.typeID);
    for (auto pos = startPos; pos < endPos; pos++) {
        ValueVectorUtils::copyNonNullDataWithSameTypeIntoPos(*outValueVector, pos - startPos,
            reinterpret_cast<uint8_t*>(inputList.overflowPtr) + pos * numOfBytes);
    }
}

bool Unwind::getNextTuplesInternal(ExecutionContext* context) {
    if (hasMoreToRead()) {
        auto totalElementsCopy = std::min(DEFAULT_VECTOR_CAPACITY, inputList.size - startIndex);
        copyTuplesToOutVector(startIndex, (totalElementsCopy + startIndex));
        startIndex += totalElementsCopy;
        outValueVector->state->initOriginalAndSelectedSize(totalElementsCopy);
        return true;
    }
    do {
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        expressionEvaluator->evaluate();
        auto pos = expressionEvaluator->resultVector->state->selVector->selectedPositions[0];
        if (expressionEvaluator->resultVector->isNull(pos)) {
            outValueVector->state->selVector->selectedSize = 0;
            continue;
        }
        inputList = expressionEvaluator->resultVector->getValue<ku_list_t>(pos);
        startIndex = 0;
        auto totalElementsCopy = std::min(DEFAULT_VECTOR_CAPACITY, inputList.size);
        copyTuplesToOutVector(0, totalElementsCopy);
        startIndex += totalElementsCopy;
        outValueVector->state->initOriginalAndSelectedSize(startIndex);
    } while (outValueVector->state->selVector->selectedSize == 0);
    return true;
}

} // namespace processor
} // namespace kuzu
