#include "storage/local_table.h"

#include "common/exception/message.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalVector::read(
    sel_t offsetInLocalVector, ValueVector* resultVector, sel_t offsetInResultVector) {
    resultVector->copyFromVectorData(offsetInResultVector, vector.get(), offsetInLocalVector);
}

void LocalVector::append(ValueVector* valueVector) {
    KU_ASSERT(valueVector->state->selVector->selectedSize == 1);
    auto pos = valueVector->state->selVector->selectedPositions[0];
    // TODO(Guodong): This should be abstracted into checkConstraints(). and performed elsewhere.
    // This is not a good place to check the size of string value. Also, this doesn't check
    // recursively for nested types. Should check during prepare commit phase?
    if (valueVector->dataType.getPhysicalType() == PhysicalTypeID::STRING) {
        auto kuStr = valueVector->getValue<ku_string_t>(pos);
        if (kuStr.len > BufferPoolConstants::PAGE_4KB_SIZE) {
            throw RuntimeException(ExceptionMessage::overLargeStringValueException(kuStr.len));
        }
    }
    vector->copyFromVectorData(numValues, valueVector, pos);
    numValues++;
}

row_idx_t LocalVectorCollection::append(ValueVector* vector) {
    prepareAppend();
    auto lastVector = vectors.back().get();
    KU_ASSERT(!lastVector->isFull());
    lastVector->append(vector);
    return numRows++;
}

void LocalVectorCollection::read(
    row_idx_t rowIdx, ValueVector* outputVector, sel_t posInOutputVector) {
    auto vectorIdx = rowIdx >> DEFAULT_VECTOR_CAPACITY_LOG_2;
    auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
    KU_ASSERT(vectorIdx < vectors.size());
    vectors[vectorIdx]->read(offsetInVector, outputVector, posInOutputVector);
}

void LocalVectorCollection::prepareAppend() {
    if (vectors.empty()) {
        vectors.emplace_back(std::make_unique<LocalVector>(*dataType, mm));
    }
    auto lastVector = vectors.back().get();
    if (lastVector->isFull()) {
        vectors.emplace_back(std::make_unique<LocalVector>(*dataType, mm));
    }
}

} // namespace storage
} // namespace kuzu
