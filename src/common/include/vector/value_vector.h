#pragma once

#include "src/common/include/data_chunk/data_chunk_state.h"
#include "src/common/include/expression_type.h"
#include "src/common/include/types.h"

namespace graphflow {
namespace common {

//! A Vector represents values of the same data type.
class ValueVector {
    friend class DataChunk;

public:
    static function<void(ValueVector&, ValueVector&)> getUnaryOperation(ExpressionType type);

    static function<void(ValueVector&, ValueVector&, ValueVector&)> getBinaryOperation(
        ExpressionType type);

    ValueVector(DataType dataType, uint64_t vectorCapacity)
        : ValueVector(vectorCapacity, getDataTypeSize(dataType), dataType) {}

    ValueVector(uint64_t numBytesPerValue, DataType dataType)
        : ValueVector(MAX_VECTOR_SIZE, numBytesPerValue, dataType) {}

    ValueVector(DataType dataType) : ValueVector(dataType, MAX_VECTOR_SIZE) {}

    inline DataType getDataType() const { return dataType; }

    inline uint8_t* getValues() const { return values; }
    inline void setValues(uint8_t* values) { this->values = values; }

    inline bool* getNullMask() const { return nullMask.get(); }

    void fillNullMask();

    template<typename T>
    void setValue(uint64_t pos, const T& value) {
        ((T*)values)[pos] = value;
    }

    template<typename T>
    void fillValue(const T& value) {
        auto vectorCapacity = capacity / getNumBytesPerValue();
        for (uint64_t i = 0; i < vectorCapacity; i++) {
            ((T*)values)[i] = value;
        }
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

    inline void setDataChunkOwnerState(shared_ptr<DataChunkState> dataChunkOwnerState) {
        this->ownerState = dataChunkOwnerState;
    }

    inline void setNumSelectedValues(uint64_t numSelectedValues) {
        ownerState->numSelectedValues = numSelectedValues;
    }

    inline uint64_t getNumSelectedValues() { return ownerState->numSelectedValues; }

    inline uint64_t* getSelectedValuesPos() { return ownerState->selectedValuesPos.get(); }

    inline uint64_t getCurrSelectedValuesPos() {
        return ownerState->selectedValuesPos[ownerState->currPos];
    }

    inline int64_t getCurrPos() { return ownerState->currPos; }

    inline uint64_t size() { return ownerState->size; }

    virtual inline int64_t getNumBytesPerValue() { return getDataTypeSize(dataType); }

    inline bool isFlat() { return ownerState->currPos != -1; }

    inline void reset() { values = buffer.get(); }

    uint8_t* reserve(size_t capacity);

protected:
    // TODO: allocate null-mask only when necessary.
    ValueVector(uint64_t vectorCapacity, uint64_t numBytesPerValue, DataType dataType)
        : capacity{numBytesPerValue * vectorCapacity},
          buffer(make_unique<uint8_t[]>(capacity)), values{buffer.get()}, dataType{dataType},
          nullMask(make_unique<bool[]>(capacity)) {
        std::fill_n(nullMask.get(), capacity, false /* not null */);
    }

    size_t capacity;
    unique_ptr<uint8_t[]> buffer;
    uint8_t* values;
    DataType dataType;
    unique_ptr<bool[]> nullMask;
    shared_ptr<DataChunkState> ownerState;
};

} // namespace common
} // namespace graphflow
