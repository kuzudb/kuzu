#pragma once

#include <cassert>
#include <numeric>

#include "common/data_chunk/data_chunk_state.h"
#include "common/null_mask.h"
#include "common/types/value.h"
#include "common/vector/auxiliary_buffer.h"

namespace kuzu {
namespace common {

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (sequence) or DEFAULT_VECTOR_CAPACITY.
class ValueVector {
    friend class ListVector;
    friend class ListAuxiliaryBuffer;
    friend class StructVector;
    friend class StringVector;

public:
    explicit ValueVector(LogicalType dataType, storage::MemoryManager* memoryManager = nullptr);
    explicit ValueVector(LogicalTypeID dataTypeID, storage::MemoryManager* memoryManager = nullptr)
        : ValueVector(LogicalType(dataTypeID), memoryManager) {
        assert(dataTypeID != LogicalTypeID::VAR_LIST);
    }

    ~ValueVector() = default;

    void setState(std::shared_ptr<DataChunkState> state_);

    inline void setAllNull() { nullMask->setAllNull(); }
    inline void setAllNonNull() { nullMask->setAllNonNull(); }
    inline void setMayContainNulls() { nullMask->setMayContainNulls(); }
    // Note that if this function returns true, there are no null. However, if it returns false, it
    // doesn't mean there are nulls, i.e., there may or may not be nulls.
    inline bool hasNoNullsGuarantee() const { return nullMask->hasNoNullsGuarantee(); }
    inline void setRangeNonNull(uint32_t startPos, uint32_t len) {
        for (auto i = 0u; i < len; ++i) {
            setNull(startPos + i, false);
        }
    }
    inline uint64_t* getNullMaskData() { return nullMask->getData(); }
    inline void setNull(uint32_t pos, bool isNull) { nullMask->setNull(pos, isNull); }
    inline uint8_t isNull(uint32_t pos) const { return nullMask->isNull(pos); }

    inline uint32_t getNumBytesPerValue() const { return numBytesPerValue; }

    template<typename T>
    inline T getValue(uint32_t pos) const {
        return ((T*)valueBuffer.get())[pos];
    }
    template<typename T>
    void setValue(uint32_t pos, T val);

    inline uint8_t* getData() const { return valueBuffer.get(); }

    inline offset_t readNodeOffset(uint32_t pos) const {
        assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
        return getValue<nodeID_t>(pos).offset;
    }

    inline void setSequential() { _isSequential = true; }
    inline bool isSequential() const { return _isSequential; }

    void resetAuxiliaryBuffer();

private:
    uint32_t getDataTypeSize(const LogicalType& type);
    void initializeValueBuffer();

public:
    LogicalType dataType;
    std::shared_ptr<DataChunkState> state;

private:
    bool _isSequential = false;
    std::unique_ptr<uint8_t[]> valueBuffer;
    std::unique_ptr<NullMask> nullMask;
    uint32_t numBytesPerValue;
    std::unique_ptr<AuxiliaryBuffer> auxiliaryBuffer;
};

class StringVector {
public:
    static inline InMemOverflowBuffer* getInMemOverflowBuffer(ValueVector* vector) {
        assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::STRING);
        return reinterpret_cast<StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getOverflowBuffer();
    }

    static inline void addString(
        common::ValueVector* vector, uint32_t pos, char* value, uint64_t len) {
        reinterpret_cast<StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->addString(vector, pos, value, len);
    }
};

class ListVector {
public:
    static inline ValueVector* getDataVector(const ValueVector* vector) {
        assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST ||
               vector->dataType.getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL);
        return reinterpret_cast<ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getDataVector();
    }
    static inline uint8_t* getListValues(const ValueVector* vector, const list_entry_t& listEntry) {
        assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST ||
               vector->dataType.getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL);
        auto dataVector = getDataVector(vector);
        return dataVector->getData() + dataVector->getNumBytesPerValue() * listEntry.offset;
    }
    static inline uint8_t* getListValuesWithOffset(const ValueVector* vector,
        const list_entry_t& listEntry, common::offset_t elementOffsetInList) {
        assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST ||
               vector->dataType.getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL);
        return getListValues(vector, listEntry) +
               elementOffsetInList * getDataVector(vector)->getNumBytesPerValue();
    }
    static inline list_entry_t addList(ValueVector* vector, uint64_t listSize) {
        assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST ||
               vector->dataType.getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL);
        return reinterpret_cast<ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->addList(listSize);
    }
};

class StructVector {
public:
    static inline const std::vector<std::shared_ptr<ValueVector>>& getChildrenVectors(
        const ValueVector* vector) {
        return reinterpret_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getChildrenVectors();
    }

    static inline std::shared_ptr<ValueVector> getChildVector(
        const ValueVector* vector, vector_idx_t idx) {
        return reinterpret_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getChildrenVectors()[idx];
    }

    static inline void referenceVector(
        ValueVector* vector, vector_idx_t idx, std::shared_ptr<ValueVector> vectorToReference) {
        reinterpret_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->referenceChildVector(idx, vectorToReference);
    }

    static inline void initializeEntries(ValueVector* vector) {
        std::iota(reinterpret_cast<int64_t*>(vector->getData()),
            reinterpret_cast<int64_t*>(
                vector->getData() + vector->getNumBytesPerValue() * DEFAULT_VECTOR_CAPACITY),
            0);
    }
};

class NodeIDVector {
public:
    // If there is still non-null values after discarding, return true. Otherwise, return false.
    // For an unflat vector, its selection vector is also updated to the resultSelVector.
    static bool discardNull(ValueVector& vector);
};

} // namespace common
} // namespace kuzu
