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
    friend class ArrowColumnVector;

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
    inline T& getValue(uint32_t pos) const {
        return ((T*)valueBuffer.get())[pos];
    }
    template<typename T>
    void setValue(uint32_t pos, T val);
    // copyFromRowData assumes rowData is non-NULL.
    void copyFromRowData(uint32_t pos, const uint8_t* rowData);
    // copyFromVectorData assumes srcVectorData is non-NULL.
    void copyToRowData(
        uint32_t pos, uint8_t* rowData, InMemOverflowBuffer* rowOverflowBuffer) const;
    void copyFromVectorData(
        uint8_t* dstData, const ValueVector* srcVector, const uint8_t* srcVectorData);

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
        assert(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
        return reinterpret_cast<StringAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getOverflowBuffer();
    }

    static void addString(ValueVector* vector, uint32_t vectorPos, ku_string_t& srcStr);
    static void addString(
        ValueVector* vector, uint32_t vectorPos, const char* srcStr, uint64_t length);
    static void addString(ValueVector* vector, ku_string_t& dstStr, ku_string_t& srcStr);
    static void addString(
        ValueVector* vector, ku_string_t& dstStr, const char* srcStr, uint64_t length);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
};

class ListVector {
public:
    static inline ValueVector* getDataVector(const ValueVector* vector) {
        assert(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return reinterpret_cast<ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getDataVector();
    }

    static inline uint64_t getDataVectorSize(const ValueVector* vector) {
        assert(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return reinterpret_cast<ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())->getSize();
    }

    static inline uint8_t* getListValues(const ValueVector* vector, const list_entry_t& listEntry) {
        assert(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        auto dataVector = getDataVector(vector);
        return dataVector->getData() + dataVector->getNumBytesPerValue() * listEntry.offset;
    }
    static inline uint8_t* getListValuesWithOffset(
        const ValueVector* vector, const list_entry_t& listEntry, offset_t elementOffsetInList) {
        assert(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return getListValues(vector, listEntry) +
               elementOffsetInList * getDataVector(vector)->getNumBytesPerValue();
    }
    static inline list_entry_t addList(ValueVector* vector, uint64_t listSize) {
        assert(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return reinterpret_cast<ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->addList(listSize);
    }

    static void copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
    static void copyFromVectorData(ValueVector* dstVector, uint8_t* dstData,
        const ValueVector* srcVector, const uint8_t* srcData);
};

class StructVector {
public:
    static inline const std::vector<std::shared_ptr<ValueVector>>& getFieldVectors(
        const ValueVector* vector) {
        return reinterpret_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getFieldVectors();
    }

    static inline std::shared_ptr<ValueVector> getFieldVector(
        const ValueVector* vector, struct_field_idx_t idx) {
        return reinterpret_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->getFieldVectors()[idx];
    }

    static inline void referenceVector(ValueVector* vector, struct_field_idx_t idx,
        std::shared_ptr<ValueVector> vectorToReference) {
        reinterpret_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->referenceChildVector(idx, vectorToReference);
    }

    static inline void initializeEntries(ValueVector* vector) {
        std::iota(reinterpret_cast<int64_t*>(vector->getData()),
            reinterpret_cast<int64_t*>(
                vector->getData() + vector->getNumBytesPerValue() * DEFAULT_VECTOR_CAPACITY),
            0);
    }

    static void copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
    static void copyFromVectorData(ValueVector* dstVector, const uint8_t* dstData,
        const ValueVector* srcVector, const uint8_t* srcData);
};

class UnionVector {
public:
    static inline ValueVector* getTagVector(const ValueVector* vector) {
        assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        return StructVector::getFieldVector(vector, 0).get();
    }

    static inline void referenceVector(ValueVector* vector, union_field_idx_t fieldIdx,
        std::shared_ptr<ValueVector> vectorToReference) {
        StructVector::referenceVector(
            vector, UnionType::getInternalFieldIdx(fieldIdx), vectorToReference);
    }

    static inline void setTagField(ValueVector* vector, union_field_idx_t tag) {
        assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
            vector->setValue<struct_field_idx_t>(
                vector->state->selVector->selectedPositions[i], tag);
        }
    }
};

class ArrowColumnVector {
public:
    static inline std::shared_ptr<arrow::Array> getArrowColumn(ValueVector* vector) {
        return reinterpret_cast<ArrowColumnAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())->column;
    }

    static void setArrowColumn(ValueVector* vector, std::shared_ptr<arrow::Array> column);
};

class NodeIDVector {
public:
    // If there is still non-null values after discarding, return true. Otherwise, return false.
    // For an unflat vector, its selection vector is also updated to the resultSelVector.
    static bool discardNull(ValueVector& vector);
};

class MapVector {
public:
    static inline ValueVector* getKeyVector(const ValueVector* vector) {
        return StructVector::getFieldVector(ListVector::getDataVector(vector), 0 /* keyVectorPos */)
            .get();
    }

    static inline ValueVector* getValueVector(const ValueVector* vector) {
        return StructVector::getFieldVector(ListVector::getDataVector(vector), 1 /* valVectorPos */)
            .get();
    }

    static inline uint8_t* getMapKeys(const ValueVector* vector, const list_entry_t& listEntry) {
        auto keyVector = getKeyVector(vector);
        return keyVector->getData() + keyVector->getNumBytesPerValue() * listEntry.offset;
    }

    static inline uint8_t* getMapValues(const ValueVector* vector, const list_entry_t& listEntry) {
        auto valueVector = getValueVector(vector);
        return valueVector->getData() + valueVector->getNumBytesPerValue() * listEntry.offset;
    }
};

} // namespace common
} // namespace kuzu
