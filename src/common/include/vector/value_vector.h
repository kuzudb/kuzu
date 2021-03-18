#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/expression_type.h"
#include "src/common/include/types.h"

namespace graphflow {
namespace common {

//! A Vector represents values of the same data type.
class ValueVector {

public:
    static function<void(ValueVector&, ValueVector&)> getUnaryOperation(ExpressionType type);

    static function<void(ValueVector&, ValueVector&, ValueVector&)> getBinaryOperation(
        ExpressionType type);

    ValueVector(const uint64_t& elementSize, const uint64_t& vectorCapacity)
        : capacity{elementSize * vectorCapacity},
          buffer(make_unique<uint8_t[]>(capacity)), values{buffer.get()} {}

    ValueVector(const uint64_t& elementSize) : ValueVector(elementSize, VECTOR_CAPACITY) {}

    ValueVector(const DataType& dataType, const uint64_t& vectorCapacity)
        : ValueVector(getDataTypeSize(dataType), vectorCapacity) {
        this->dataType = dataType;
    }

    ValueVector(const DataType& dataType) : ValueVector(getDataTypeSize(dataType)) {
        this->dataType = dataType;
    }

    inline DataType getDataType() const { return dataType; }

    inline uint8_t* getValues() const { return values; }
    inline void setValues(uint8_t* values) { this->values = values; }

    template<typename T>
    void setValue(const uint64_t& pos, const T& value) {
        ((T*)values)[pos] = value;
    }

    template<typename T>
    T getValue(uint64_t pos) {
        if (pos >= VECTOR_CAPACITY) {
            throw invalid_argument("Position out of bound.");
        }
        return ((T*)values)[pos];
    }

    inline void setDataChunkOwner(shared_ptr<DataChunk>& owner) { this->owner = owner; }

    inline int64_t getCurrPos() { return owner->getCurrPos(); }

    inline uint64_t size() { return owner->getNumTuples(); }

    virtual inline int64_t getElementSize() { return getDataTypeSize(dataType); }

    inline bool isFlat() { return owner->isFlat(); }

    inline void reset() { values = buffer.get(); }

    uint8_t* reserve(size_t capacity);

public:
    //! The capacity of vector values is dependent on how the vector is produced.
    //!  Scans produce vectors in chunks of 1024 nodes while extends leads to the
    //!  the max size of an adjacency list which is 2048 nodes.
    constexpr static size_t VECTOR_CAPACITY = 2048;
    constexpr static size_t NODE_SEQUENCE_VECTOR_SIZE = 1024;

protected:
    size_t capacity;
    unique_ptr<uint8_t[]> buffer;
    uint8_t* values;
    shared_ptr<DataChunk> owner;
    DataType dataType;
};

} // namespace common
} // namespace graphflow
