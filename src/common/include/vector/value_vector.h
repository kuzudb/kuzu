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

    ValueVector(DataType dataType, uint64_t vectorCapacity)
        : capacity{getDataTypeSize(dataType) * vectorCapacity},
          buffer(make_unique<uint8_t[]>(capacity)), values{buffer.get()}, dataType{dataType} {}

    ValueVector(uint64_t numBytesPerValue, DataType dataType)
        : ValueVector(MAX_VECTOR_SIZE, numBytesPerValue, dataType) {}

    ValueVector(DataType dataType) : ValueVector(dataType, MAX_VECTOR_SIZE) {}

    inline DataType getDataType() const { return dataType; }

    inline uint8_t* getValues() const { return values; }
    inline void setValues(uint8_t* values) { this->values = values; }

    template<typename T>
    void setValue(uint64_t pos, const T& value) {
        ((T*)values)[pos] = value;
    }

    virtual void readNodeOffset(uint64_t pos, nodeID_t& nodeID) {
        throw invalid_argument("not supported.");
    }

    virtual void readNodeOffsetAndLabel(uint64_t pos, nodeID_t& nodeID) {
        throw invalid_argument("not supported.");
    }

    template<typename T>
    T getValue(uint64_t pos) {
        return ((T*)values)[pos];
    }

    inline void setDataChunkOwner(shared_ptr<DataChunk>& owner) { this->owner = owner; }

    inline uint64_t getNumSelectedValues() { return owner->numSelectedValues; }

    inline uint64_t* getSelectedValuesPos() { return owner->selectedValuesPos.get(); }

    inline uint64_t getCurrSelectedValuesPos() { return owner->getCurrSelectedValuesPos(); }

    inline int64_t getCurrPos() { return owner->selectedValuesPos[owner->currPos]; }

    inline uint64_t size() { return owner->size; }

    virtual inline int64_t getNumBytesPerValue() { return getDataTypeSize(dataType); }

    inline bool isFlat() { return owner->isFlat(); }

    inline void reset() { values = buffer.get(); }

    uint8_t* reserve(size_t capacity);

public:
    shared_ptr<DataChunk> owner;

protected:
    ValueVector(uint64_t vectorCapacity, uint64_t numBytesPerValue, DataType dataType)
        : capacity{numBytesPerValue * vectorCapacity},
          buffer(make_unique<uint8_t[]>(capacity)), values{buffer.get()}, dataType{dataType} {}

    size_t capacity;
    unique_ptr<uint8_t[]> buffer;
    uint8_t* values;
    DataType dataType;
};

} // namespace common
} // namespace graphflow
