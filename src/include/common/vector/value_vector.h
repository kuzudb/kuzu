#pragma once

#include <numeric>
#include <utility>

#include "common/assert.h"
#include "common/cast.h"
#include "common/data_chunk/data_chunk_state.h"
#include "common/null_mask.h"
#include "common/types/ku_string.h"
#include "common/vector/auxiliary_buffer.h"

namespace kuzu {
namespace common {

class Value;

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (sequence) or DEFAULT_VECTOR_CAPACITY.
class KUZU_API ValueVector {
    friend class ListVector;
    friend class ListAuxiliaryBuffer;
    friend class StructVector;
    friend class StringVector;
    friend class ArrowColumnVector;

public:
    explicit ValueVector(LogicalType dataType, storage::MemoryManager* memoryManager = nullptr);
    explicit ValueVector(LogicalTypeID dataTypeID, storage::MemoryManager* memoryManager = nullptr)
        : ValueVector(LogicalType(dataTypeID), memoryManager) {
        KU_ASSERT(dataTypeID != LogicalTypeID::VAR_LIST);
    }

    ~ValueVector() = default;

    void setState(const std::shared_ptr<DataChunkState>& state_);

    void setAllNull() { nullMask->setAllNull(); }
    void setAllNonNull() { nullMask->setAllNonNull(); }
    // On return true, there are no null. On return false, there may or may not be nulls.
    bool hasNoNullsGuarantee() const { return nullMask->hasNoNullsGuarantee(); }
    void setNullRange(uint32_t startPos, uint32_t len, bool value) {
        nullMask->setNullFromRange(startPos, len, value);
    }
    const uint64_t* getNullMaskData() { return nullMask->getData(); }
    void setNull(uint32_t pos, bool isNull);
    uint8_t isNull(uint32_t pos) const { return nullMask->isNull(pos); }
    void setAsSingleNullEntry() {
        state->selVector->selectedSize = 1;
        setNull(state->selVector->selectedPositions[0], true);
    }

    bool setNullFromBits(const uint64_t* srcNullEntries, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBitsToCopy, bool invert = false);

    uint32_t getNumBytesPerValue() const { return numBytesPerValue; }

    // TODO(Guodong): Rename this to getValueRef
    template<typename T>
    const T& getValue(uint32_t pos) const {
        return ((T*)valueBuffer.get())[pos];
    }
    template<typename T>
    T& getValue(uint32_t pos) {
        return ((T*)valueBuffer.get())[pos];
    }
    template<typename T>
    void setValue(uint32_t pos, T val);
    // copyFromRowData assumes rowData is non-NULL.
    void copyFromRowData(uint32_t pos, const uint8_t* rowData);
    // copyToRowData assumes srcVectorData is non-NULL.
    void copyToRowData(
        uint32_t pos, uint8_t* rowData, InMemOverflowBuffer* rowOverflowBuffer) const;
    // copyFromVectorData assumes srcVectorData is non-NULL.
    void copyFromVectorData(
        uint8_t* dstData, const ValueVector* srcVector, const uint8_t* srcVectorData);
    void copyFromVectorData(uint64_t dstPos, const ValueVector* srcVector, uint64_t srcPos);
    void copyFromValue(uint64_t pos, const Value& value);

    std::unique_ptr<Value> getAsValue(uint64_t pos) const;

    uint8_t* getData() const { return valueBuffer.get(); }

    offset_t readNodeOffset(uint32_t pos) const {
        KU_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
        return getValue<nodeID_t>(pos).offset;
    }

    void setSequential() { _isSequential = true; }
    bool isSequential() const { return _isSequential; }

    void resetAuxiliaryBuffer();

    // If there is still non-null values after discarding, return true. Otherwise, return false.
    // For an unflat vector, its selection vector is also updated to the resultSelVector.
    static bool discardNull(ValueVector& vector);

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

class KUZU_API StringVector {
public:
    static inline InMemOverflowBuffer* getInMemOverflowBuffer(ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
        return ku_dynamic_cast<AuxiliaryBuffer*, StringAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getOverflowBuffer();
    }

    static void addString(ValueVector* vector, uint32_t vectorPos, ku_string_t& srcStr);
    static void addString(
        ValueVector* vector, uint32_t vectorPos, const char* srcStr, uint64_t length);
    static void addString(ValueVector* vector, uint32_t vectorPos, const std::string& srcStr);
    // Add empty string with space reserved for the provided size
    // Returned value can be modified to set the string contents
    static ku_string_t& reserveString(ValueVector* vector, uint32_t vectorPos, uint64_t length);
    static void reserveString(ValueVector* vector, ku_string_t& dstStr, uint64_t length);
    static void addString(ValueVector* vector, ku_string_t& dstStr, ku_string_t& srcStr);
    static void addString(
        ValueVector* vector, ku_string_t& dstStr, const char* srcStr, uint64_t length);
    static void addString(
        kuzu::common::ValueVector* vector, ku_string_t& dstStr, const std::string& srcStr);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
};

struct KUZU_API BlobVector {
    static void addBlob(ValueVector* vector, uint32_t pos, const char* data, uint32_t length) {
        StringVector::addString(vector, pos, data, length);
    }
    static void addBlob(ValueVector* vector, uint32_t pos, const uint8_t* data, uint64_t length) {
        StringVector::addString(vector, pos, reinterpret_cast<const char*>(data), length);
    }
};

class KUZU_API ListVector {
public:
    static void setDataVector(const ValueVector* vector, std::shared_ptr<ValueVector> dataVector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        auto listBuffer =
            ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get());
        listBuffer->setDataVector(std::move(dataVector));
    }
    static ValueVector* getDataVector(const ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getDataVector();
    }
    static std::shared_ptr<ValueVector> getSharedDataVector(const ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getSharedDataVector();
    }
    static uint64_t getDataVectorSize(const ValueVector* vector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getSize();
    }

    static uint8_t* getListValues(const ValueVector* vector, const list_entry_t& listEntry) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        auto dataVector = getDataVector(vector);
        return dataVector->getData() + dataVector->getNumBytesPerValue() * listEntry.offset;
    }
    static uint8_t* getListValuesWithOffset(
        const ValueVector* vector, const list_entry_t& listEntry, offset_t elementOffsetInList) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return getListValues(vector, listEntry) +
               elementOffsetInList * getDataVector(vector)->getNumBytesPerValue();
    }
    static list_entry_t addList(ValueVector* vector, uint64_t listSize) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
        return ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->addList(listSize);
    }
    static void resizeDataVector(ValueVector* vector, uint64_t numValues) {
        ku_dynamic_cast<AuxiliaryBuffer*, ListAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->resize(numValues);
    }

    static void copyFromRowData(ValueVector* vector, uint32_t pos, const uint8_t* rowData);
    static void copyToRowData(const ValueVector* vector, uint32_t pos, uint8_t* rowData,
        InMemOverflowBuffer* rowOverflowBuffer);
    static void copyFromVectorData(ValueVector* dstVector, uint8_t* dstData,
        const ValueVector* srcVector, const uint8_t* srcData);
    static void appendDataVector(
        ValueVector* dstVector, ValueVector* srcDataVector, uint64_t numValuesToAppend);
    static void sliceDataVector(ValueVector* vectorToSlice, uint64_t offset, uint64_t numValues);
};

class StructVector {
public:
    static inline const std::vector<std::shared_ptr<ValueVector>>& getFieldVectors(
        const ValueVector* vector) {
        return ku_dynamic_cast<AuxiliaryBuffer*, StructAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getFieldVectors();
    }

    static inline std::shared_ptr<ValueVector> getFieldVector(
        const ValueVector* vector, struct_field_idx_t idx) {
        return ku_dynamic_cast<AuxiliaryBuffer*, StructAuxiliaryBuffer*>(
            vector->auxiliaryBuffer.get())
            ->getFieldVectors()[idx];
    }

    static inline void referenceVector(ValueVector* vector, struct_field_idx_t idx,
        std::shared_ptr<ValueVector> vectorToReference) {
        ku_dynamic_cast<AuxiliaryBuffer*, StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
            ->referenceChildVector(idx, std::move(vectorToReference));
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
        KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        return StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX).get();
    }

    static inline ValueVector* getValVector(const ValueVector* vector, union_field_idx_t fieldIdx) {
        KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        return StructVector::getFieldVector(vector, UnionType::getInternalFieldIdx(fieldIdx)).get();
    }

    static inline void referenceVector(ValueVector* vector, union_field_idx_t fieldIdx,
        std::shared_ptr<ValueVector> vectorToReference) {
        StructVector::referenceVector(
            vector, UnionType::getInternalFieldIdx(fieldIdx), std::move(vectorToReference));
    }

    static inline void setTagField(ValueVector* vector, union_field_idx_t tag) {
        KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
        for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
            vector->setValue<struct_field_idx_t>(
                vector->state->selVector->selectedPositions[i], tag);
        }
    }
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

struct RdfVariantVector {
    static void addString(ValueVector* vector, sel_t pos, ku_string_t str);
    static void addString(ValueVector* vector, sel_t pos, const char* str, uint32_t length);

    template<typename T>
    static void add(ValueVector* vector, sel_t pos, T val);
};

} // namespace common
} // namespace kuzu
