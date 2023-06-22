#include "processor/operator/unwind.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Unwind::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    expressionEvaluator->init(*resultSet, context->memoryManager);
    outValueVector = resultSet->getValueVector(outDataPos);
}

bool Unwind::hasMoreToRead() const {
    return listEntry.offset != INVALID_OFFSET && listEntry.size > startIndex;
}

void Unwind::copyTuplesToOutVector(uint64_t startPos, uint64_t endPos) const {
    auto listValues = common::ListVector::getListValuesWithOffset(
        expressionEvaluator->resultVector.get(), listEntry, startPos);
    auto listDataVector =
        common::ListVector::getDataVector(expressionEvaluator->resultVector.get());
    for (auto pos = startPos; pos < endPos; pos++) {
        outValueVector->copyFromVectorData(
            outValueVector->getData() + outValueVector->getNumBytesPerValue() * (pos - startPos),
            listDataVector, listValues);
        listValues += listDataVector->getNumBytesPerValue();
    }
}

bool Unwind::getNextTuplesInternal(ExecutionContext* context) {
    if (hasMoreToRead()) {
        auto totalElementsCopy = std::min(DEFAULT_VECTOR_CAPACITY, listEntry.size - startIndex);
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
        listEntry = expressionEvaluator->resultVector->getValue<common::list_entry_t>(pos);
        startIndex = 0;
        auto totalElementsCopy = std::min(DEFAULT_VECTOR_CAPACITY, listEntry.size);
        copyTuplesToOutVector(0, totalElementsCopy);
        startIndex += totalElementsCopy;
        outValueVector->state->initOriginalAndSelectedSize(startIndex);
    } while (outValueVector->state->selVector->selectedSize == 0);
    return true;
}

} // namespace processor
} // namespace kuzu
