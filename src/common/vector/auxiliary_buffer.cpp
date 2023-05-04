#include "common/vector/auxiliary_buffer.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

void StringAuxiliaryBuffer::addString(
    common::ValueVector* vector, uint32_t pos, char* value, uint64_t len) const {
    assert(vector->dataType.typeID == STRING);
    auto& entry = ((ku_string_t*)vector->getData())[pos];
    InMemOverflowBufferUtils::copyString(value, len, entry, *inMemOverflowBuffer);
}

ListAuxiliaryBuffer::ListAuxiliaryBuffer(
    kuzu::common::DataType& dataVectorType, storage::MemoryManager* memoryManager)
    : capacity{common::DEFAULT_VECTOR_CAPACITY}, size{0}, dataVector{std::make_unique<ValueVector>(
                                                              dataVectorType, memoryManager)} {}

list_entry_t ListAuxiliaryBuffer::addList(uint64_t listSize) {
    auto listEntry = list_entry_t{size, listSize};
    bool needResizeDataVector = size + listSize > capacity;
    while (size + listSize > capacity) {
        capacity *= 2;
    }
    auto numBytesPerElement = dataVector->getNumBytesPerValue();
    if (needResizeDataVector) {
        auto buffer = std::make_unique<uint8_t[]>(capacity * numBytesPerElement);
        memcpy(dataVector->valueBuffer.get(), buffer.get(), size * numBytesPerElement);
        dataVector->valueBuffer = std::move(buffer);
        dataVector->nullMask->resize(capacity);
    }
    size += listSize;
    return listEntry;
}

std::unique_ptr<AuxiliaryBuffer> AuxiliaryBufferFactory::getAuxiliaryBuffer(
    DataType& type, storage::MemoryManager* memoryManager) {
    switch (type.typeID) {
    case STRING:
        return std::make_unique<StringAuxiliaryBuffer>(memoryManager);
    case STRUCT:
        return std::make_unique<StructAuxiliaryBuffer>();
    case VAR_LIST:
        return std::make_unique<ListAuxiliaryBuffer>(*type.getChildType(), memoryManager);
    default:
        return nullptr;
    }
}

} // namespace common
} // namespace kuzu
