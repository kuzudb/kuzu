#pragma once

#include "arrow/array.h"
#include "common/in_mem_overflow_buffer.h"

namespace kuzu {
namespace common {

class ValueVector;

// AuxiliaryBuffer holds data which is only used by the targeting dataType.
class AuxiliaryBuffer {
public:
    virtual ~AuxiliaryBuffer() = default;
};

class StringAuxiliaryBuffer : public AuxiliaryBuffer {
public:
    explicit StringAuxiliaryBuffer(storage::MemoryManager* memoryManager) {
        inMemOverflowBuffer = std::make_unique<InMemOverflowBuffer>(memoryManager);
    }

    inline InMemOverflowBuffer* getOverflowBuffer() const { return inMemOverflowBuffer.get(); }
    inline void resetOverflowBuffer() const { inMemOverflowBuffer->resetBuffer(); }
    void addString(common::ValueVector* vector, uint32_t pos, char* value, uint64_t len) const;

private:
    std::unique_ptr<InMemOverflowBuffer> inMemOverflowBuffer;
};

class StructAuxiliaryBuffer : public AuxiliaryBuffer {
public:
    StructAuxiliaryBuffer(const LogicalType& type, storage::MemoryManager* memoryManager);

    inline void referenceChildVector(
        vector_idx_t idx, std::shared_ptr<ValueVector> vectorToReference) {
        childrenVectors[idx] = std::move(vectorToReference);
    }
    inline const std::vector<std::shared_ptr<ValueVector>>& getFieldVectors() const {
        return childrenVectors;
    }

private:
    std::vector<std::shared_ptr<ValueVector>> childrenVectors;
};

class ArrowColumnAuxiliaryBuffer : public AuxiliaryBuffer {
    friend class ArrowColumnVector;

private:
    std::shared_ptr<arrow::Array> column;
};

// ListVector layout:
// To store a list value in the valueVector, we could use two separate vectors.
// 1. A vector(called offset vector) for the list offsets and length(called list_entry_t): This
// vector contains the starting indices and length for each list within the data vector.
// 2. A data vector(called dataVector) to store the actual list elements: This vector holds the
// actual elements of the lists in a flat, continuous storage. Each list would be represented as a
// contiguous subsequence of elements in this vector.
class ListAuxiliaryBuffer : public AuxiliaryBuffer {
public:
    ListAuxiliaryBuffer(const LogicalType& dataVectorType, storage::MemoryManager* memoryManager);

    inline ValueVector* getDataVector() const { return dataVector.get(); }

    list_entry_t addList(uint64_t listSize);

    inline uint64_t getSize() const { return size; }

    inline void resetSize() { size = 0; }

private:
    void resizeDataVector(ValueVector* dataVector);

private:
    uint64_t capacity;
    uint64_t size;
    std::unique_ptr<ValueVector> dataVector;
};

class AuxiliaryBufferFactory {
public:
    static std::unique_ptr<AuxiliaryBuffer> getAuxiliaryBuffer(
        LogicalType& type, storage::MemoryManager* memoryManager);
};

} // namespace common
} // namespace kuzu
