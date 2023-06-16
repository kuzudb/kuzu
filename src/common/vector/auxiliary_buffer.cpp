#include "common/vector/auxiliary_buffer.h"

#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

StructAuxiliaryBuffer::StructAuxiliaryBuffer(
    const LogicalType& type, storage::MemoryManager* memoryManager) {
    auto fieldTypes = StructType::getFieldTypes(&type);
    childrenVectors.reserve(fieldTypes.size());
    for (auto fieldType : fieldTypes) {
        childrenVectors.push_back(std::make_shared<ValueVector>(*fieldType, memoryManager));
    }
}

ListAuxiliaryBuffer::ListAuxiliaryBuffer(
    const LogicalType& dataVectorType, storage::MemoryManager* memoryManager)
    : capacity{common::DEFAULT_VECTOR_CAPACITY}, size{0}, dataVector{std::make_unique<ValueVector>(
                                                              dataVectorType, memoryManager)} {}

list_entry_t ListAuxiliaryBuffer::addList(uint64_t listSize) {
    auto listEntry = list_entry_t{size, listSize};
    bool needResizeDataVector = size + listSize > capacity;
    while (size + listSize > capacity) {
        capacity *= 2;
    }
    if (needResizeDataVector) {
        resizeDataVector(dataVector.get());
    }
    size += listSize;
    return listEntry;
}

void ListAuxiliaryBuffer::resizeDataVector(ValueVector* dataVector) {
    // If the dataVector is a struct vector, we need to resize its field vectors.
    if (dataVector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT) {
        auto fieldVectors = StructVector::getFieldVectors(dataVector);
        for (auto& fieldVector : fieldVectors) {
            resizeDataVector(fieldVector.get());
        }
    } else {
        auto buffer = std::make_unique<uint8_t[]>(capacity * dataVector->getNumBytesPerValue());
        memcpy(
            buffer.get(), dataVector->valueBuffer.get(), size * dataVector->getNumBytesPerValue());
        dataVector->valueBuffer = std::move(buffer);
        dataVector->nullMask->resize(capacity);
    }
}

std::unique_ptr<AuxiliaryBuffer> AuxiliaryBufferFactory::getAuxiliaryBuffer(
    LogicalType& type, storage::MemoryManager* memoryManager) {
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::STRING:
        return std::make_unique<StringAuxiliaryBuffer>(memoryManager);
    case PhysicalTypeID::STRUCT:
        return std::make_unique<StructAuxiliaryBuffer>(type, memoryManager);
    case PhysicalTypeID::VAR_LIST:
        return std::make_unique<ListAuxiliaryBuffer>(
            *VarListType::getChildType(&type), memoryManager);
    case PhysicalTypeID::ARROW_COLUMN:
        return std::make_unique<ArrowColumnAuxiliaryBuffer>();
    default:
        return nullptr;
    }
}

} // namespace common
} // namespace kuzu
