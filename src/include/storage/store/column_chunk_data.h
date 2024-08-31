#pragma once

#include <functional>
#include <variant>

#include "common/constants.h"
#include "common/data_chunk/sel_vector.h"
#include "common/enums/rel_multiplicity.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "storage/compression/compression.h"
#include "storage/enums/residency_state.h"
#include "storage/store/column_chunk_metadata.h"
#include "storage/store/column_reader_writer.h"
#include "storage/store/in_memory_exception_chunk.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator

namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

class Column;
class NullChunkData;

// TODO(bmwinger): Hide access to variables.
struct ChunkState {
    Column* column;
    ColumnChunkMetadata metadata;
    uint64_t numValuesPerPage = UINT64_MAX;
    std::unique_ptr<ChunkState> nullState;

    // Used for struct/list/string columns.
    std::vector<ChunkState> childrenStates;

    // Used for floating point columns
    std::variant<std::unique_ptr<InMemoryExceptionChunk<double>>,
        std::unique_ptr<InMemoryExceptionChunk<float>>>
        alpExceptionChunk;

    explicit ChunkState(bool hasNull = true) : column{nullptr} {
        if (hasNull) {
            nullState = std::make_unique<ChunkState>(false /*hasNull*/);
        }
    }
    ChunkState(ColumnChunkMetadata metadata, uint64_t numValuesPerPage)
        : column{nullptr}, metadata{std::move(metadata)}, numValuesPerPage{numValuesPerPage} {
        nullState = std::make_unique<ChunkState>(false /*hasNull*/);
    }

    ChunkState& getChildState(common::idx_t childIdx) {
        KU_ASSERT(childIdx < childrenStates.size());
        return childrenStates[childIdx];
    }
    const ChunkState& getChildState(common::idx_t childIdx) const {
        KU_ASSERT(childIdx < childrenStates.size());
        return childrenStates[childIdx];
    }

    void resetState() {
        numValuesPerPage = UINT64_MAX;
        if (nullState) {
            nullState->resetState();
        }
        for (auto& childState : childrenStates) {
            childState.resetState();
        }
    }

    template<std::floating_point T>
    InMemoryExceptionChunk<T>* getExceptionChunk() {
        using GetType = std::unique_ptr<InMemoryExceptionChunk<T>>;
        KU_ASSERT(std::holds_alternative<GetType>(alpExceptionChunk));
        return std::get<GetType>(alpExceptionChunk).get();
    }

    template<std::floating_point T>
    const InMemoryExceptionChunk<T>* getExceptionChunkConst() const {
        using GetType = std::unique_ptr<InMemoryExceptionChunk<T>>;
        KU_ASSERT(std::holds_alternative<GetType>(alpExceptionChunk));
        return std::get<GetType>(alpExceptionChunk).get();
    }
};

class FileHandle;
// Base data segment covers all fixed-sized data types.
class ColumnChunkData {
public:
    friend struct ColumnChunkFactory;

    ColumnChunkData(common::LogicalType dataType, uint64_t capacity, bool enableCompression,
        ResidencyState residencyState, bool hasNullData);
    ColumnChunkData(common::LogicalType dataType, bool enableCompression,
        const ColumnChunkMetadata& metadata, bool hasNullData);
    ColumnChunkData(common::PhysicalTypeID physicalType, bool enableCompression,
        const ColumnChunkMetadata& metadata, bool hasNullData);
    virtual ~ColumnChunkData() = default;

    template<typename T>
    T getValue(common::offset_t pos) const {
        KU_ASSERT(pos < numValues);
        KU_ASSERT(residencyState != ResidencyState::ON_DISK);
        return reinterpret_cast<T*>(buffer.get())[pos];
    }
    template<typename T>
    void setValue(T val, common::offset_t pos) {
        KU_ASSERT(pos < capacity);
        KU_ASSERT(residencyState != ResidencyState::ON_DISK);
        reinterpret_cast<T*>(buffer.get())[pos] = val;
        if (pos >= numValues) {
            numValues = pos + 1;
        }
    }

    bool isNull(common::offset_t pos) const;
    void setNullData(std::unique_ptr<NullChunkData> nullData_) { nullData = std::move(nullData_); }
    bool hasNullData() const { return nullData != nullptr; }
    NullChunkData* getNullData() { return nullData.get(); }
    const NullChunkData& getNullData() const { return *nullData; }
    std::optional<common::NullMask> getNullMask() const;
    std::unique_ptr<NullChunkData> moveNullData() { return std::move(nullData); }

    common::LogicalType& getDataType() { return dataType; }
    const common::LogicalType& getDataType() const { return dataType; }
    ResidencyState getResidencyState() const { return residencyState; }
    bool isCompressionEnabled() const { return enableCompression; }
    ColumnChunkMetadata& getMetadata() {
        KU_ASSERT(residencyState == ResidencyState::ON_DISK);
        return metadata;
    }
    const ColumnChunkMetadata& getMetadata() const {
        KU_ASSERT(residencyState == ResidencyState::ON_DISK);
        return metadata;
    }
    void setMetadata(const ColumnChunkMetadata& metadata_) {
        KU_ASSERT(residencyState == ResidencyState::ON_DISK);
        metadata = metadata_;
    }

    // Only have side effects on in-memory or temporary chunks.
    virtual void resetToAllNull();
    virtual void resetToEmpty();

    // Note that the startPageIdx is not known, so it will always be common::INVALID_PAGE_IDX
    virtual ColumnChunkMetadata getMetadataToFlush() const;

    virtual void append(common::ValueVector* vector, const common::SelectionVector& selVector);
    virtual void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    virtual void flush(FileHandle& dataFH);

    ColumnChunkMetadata flushBuffer(FileHandle* dataFH, common::page_idx_t startPageIdx,
        const ColumnChunkMetadata& metadata) const;

    static common::page_idx_t getNumPagesForBytes(uint64_t numBytes) {
        return (numBytes + common::PAGE_SIZE - 1) / common::PAGE_SIZE;
    }

    uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    uint8_t* getData() const { return buffer.get(); }

    virtual void initializeScanState(ChunkState& state, Column* column) const;
    virtual void scan(common::ValueVector& output, common::offset_t offset, common::length_t length,
        common::sel_t posInOutputVector = 0) const;
    virtual void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const;

    // TODO(Guodong): In general, this is not a good interface. Instead of passing in
    // `offsetInVector`, we should flatten the vector to pos at `offsetInVector`.
    virtual void write(const common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk);
    virtual void write(ColumnChunkData* chunk, ColumnChunkData* offsetsInChunk,
        common::RelMultiplicity multiplicity);
    virtual void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy);
    // TODO(Guodong): Used in `applyDeletionsToChunk`. Should unify with `write`.
    virtual void copy(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy);

    virtual void setToInMemory();
    // numValues must be at least the number of values the ColumnChunk was first initialized
    // with
    virtual void resize(uint64_t newCapacity);

    void populateWithDefaultVal(evaluator::ExpressionEvaluator& defaultEvaluator,
        uint64_t& numValues_);
    virtual void finalize() {
        KU_ASSERT(residencyState != ResidencyState::ON_DISK);
        // DO NOTHING.
    }

    uint64_t getCapacity() const { return capacity; }
    virtual uint64_t getNumValues() const { return numValues; }
    // TODO(Guodong): Alternatively, we can let `getNumValues` read from metadata when ON_DISK.
    virtual void resetNumValuesFromMetadata();
    virtual void setNumValues(uint64_t numValues_);
    virtual void syncNumValues() {}
    virtual bool numValuesSanityCheck() const;

    virtual bool sanityCheck() const;

    virtual uint64_t getEstimatedMemoryUsage() const;

    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<ColumnChunkData> deserialize(common::Deserializer& deSer);

    template<typename TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<ColumnChunkData&, TARGET&>(*this);
    }
    template<typename TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<const ColumnChunkData&, const TARGET&>(*this);
    }

protected:
    // Initializes the data buffer and functions. They are (and should be) only called in
    // constructor.
    void initializeBuffer(common::PhysicalTypeID physicalType);
    void initializeFunction(bool enableCompression);

    // Note: This function is not setting child/null chunk data recursively.
    void setToOnDisk(const ColumnChunkMetadata& metadata);

    virtual void copyVectorToBuffer(common::ValueVector* vector, common::offset_t startPosInChunk,
        const common::SelectionVector& selVector);

private:
    uint64_t getBufferSize(uint64_t capacity_) const;

protected:
    using flush_buffer_func_t = std::function<ColumnChunkMetadata(const uint8_t*, uint64_t,
        FileHandle*, common::page_idx_t, const ColumnChunkMetadata&)>;
    using get_metadata_func_t = std::function<ColumnChunkMetadata(const uint8_t*, uint64_t,
        uint64_t, uint64_t, StorageValue, StorageValue)>;
    using get_min_max_func_t =
        std::function<std::pair<StorageValue, StorageValue>(const uint8_t*, uint64_t)>;

    ResidencyState residencyState;
    common::LogicalType dataType;
    bool enableCompression;
    uint32_t numBytesPerValue;
    uint64_t bufferSize;
    uint64_t capacity;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<NullChunkData> nullData;
    uint64_t numValues;
    flush_buffer_func_t flushBufferFunction;
    get_metadata_func_t getMetadataFunction;

    // On-disk metadata for column chunk.
    ColumnChunkMetadata metadata;
};

template<>
inline void ColumnChunkData::setValue(bool val, common::offset_t pos) {
    // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
    common::NullMask::setNull(reinterpret_cast<uint64_t*>(buffer.get()), pos, val);
}

template<>
inline bool ColumnChunkData::getValue(common::offset_t pos) const {
    // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
    return common::NullMask::isNull(reinterpret_cast<uint64_t*>(buffer.get()), pos);
}

// Stored as bitpacked booleans in-memory and on-disk
class BoolChunkData : public ColumnChunkData {
public:
    BoolChunkData(uint64_t capacity, bool enableCompression, ResidencyState type, bool hasNullChunk)
        : ColumnChunkData(common::LogicalType::BOOL(), capacity,
              // Booleans are always bitpacked, but this can also enable constant compression
              enableCompression, type, hasNullChunk) {}
    BoolChunkData(bool enableCompression, const ColumnChunkMetadata& metadata, bool hasNullData)
        : ColumnChunkData{common::LogicalType::BOOL(), enableCompression, metadata, hasNullData} {}

    void append(common::ValueVector* vector, const common::SelectionVector& sel) final;
    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;

    void scan(common::ValueVector& output, common::offset_t offset, common::length_t length,
        common::sel_t posInOutputVector = 0) const override;
    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    void write(const common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
        common::RelMultiplicity multiplicity) final;
    void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
};

class NullChunkData final : public BoolChunkData {
public:
    NullChunkData(uint64_t capacity, bool enableCompression, ResidencyState type)
        : BoolChunkData(capacity, enableCompression, type, false /*hasNullData*/),
          mayHaveNullValue{false} {}
    NullChunkData(bool enableCompression, const ColumnChunkMetadata& metadata)
        : BoolChunkData{enableCompression, metadata, false /*hasNullData*/},
          mayHaveNullValue{false} {}

    // Maybe this should be combined with BoolChunkData if the only difference is these
    // functions?
    bool isNull(common::offset_t pos) const { return getValue<bool>(pos); }
    void setNull(common::offset_t pos, bool isNull);

    bool mayHaveNull() const { return mayHaveNullValue; }

    void resetToEmpty() override {
        resetToNoNull();
        numValues = 0;
    }
    void resetToNoNull() {
        memset(buffer.get(), 0 /* non null */, bufferSize);
        mayHaveNullValue = false;
    }
    void resetToAllNull() override {
        memset(buffer.get(), 0xFF /* null */, bufferSize);
        mayHaveNullValue = true;
    }

    void copyFromBuffer(uint64_t* srcBuffer, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBits, bool invert = false) {
        if (common::NullMask::copyNullMask(srcBuffer, srcOffset,
                reinterpret_cast<uint64_t*>(buffer.get()), dstOffset, numBits, invert)) {
            mayHaveNullValue = true;
        }
    }

    // NullChunkData::scan updates the null mask of output vector
    void scan(common::ValueVector& output, common::offset_t offset, common::length_t length,
        common::sel_t posInOutputVector = 0) const override;

    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;

    void write(const common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<NullChunkData> deserialize(common::Deserializer& deSer);

    common::NullMask getNullMask() const {
        return common::NullMask(std::span(reinterpret_cast<uint64_t*>(buffer.get()), capacity / 64),
            mayHaveNullValue);
    }

protected:
    bool mayHaveNullValue;
};

class InternalIDChunkData final : public ColumnChunkData {
public:
    // TODO(Guodong): Should make InternalIDChunkData has no NULL.
    // Physically, we only materialize offset of INTERNAL_ID, which is same as UINT64,
    InternalIDChunkData(uint64_t capacity, bool enableCompression, ResidencyState residencyState)
        : ColumnChunkData(common::LogicalType::INTERNAL_ID(), capacity, enableCompression,
              residencyState, false /*hasNullData*/),
          commonTableID{common::INVALID_TABLE_ID} {}
    InternalIDChunkData(bool enableCompression, const ColumnChunkMetadata& metadata)
        : ColumnChunkData{common::LogicalType::INTERNAL_ID(), enableCompression, metadata,
              false /*hasNullData*/},
          commonTableID{common::INVALID_TABLE_ID} {}

    void append(common::ValueVector* vector, const common::SelectionVector& selVector) override;

    void copyVectorToBuffer(common::ValueVector* vector, common::offset_t startPosInChunk,
        const common::SelectionVector& selVector) override;

    void copyInt64VectorToBuffer(common::ValueVector* vector, common::offset_t startPosInChunk,
        const common::SelectionVector& selVector) const;

    void scan(common::ValueVector& output, common::offset_t offset, common::length_t length,
        common::sel_t posInOutputVector = 0) const override;
    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    void write(const common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;

    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;

    void setTableID(common::table_id_t tableID) { commonTableID = tableID; }
    common::table_id_t getTableID() const { return commonTableID; }

    common::offset_t operator[](common::offset_t pos) const {
        return getValue<common::offset_t>(pos);
    }
    common::offset_t& operator[](common::offset_t pos) {
        return reinterpret_cast<common::offset_t*>(buffer.get())[pos];
    }

private:
    common::table_id_t commonTableID;
};

struct ColumnChunkFactory {
    static std::unique_ptr<ColumnChunkData> createColumnChunkData(common::LogicalType dataType,
        bool enableCompression, uint64_t capacity, ResidencyState residencyState,
        bool hasNullData = true);
    static std::unique_ptr<ColumnChunkData> createColumnChunkData(common::LogicalType dataType,
        bool enableCompression, ColumnChunkMetadata& metadata, bool hasNullData);

    static std::unique_ptr<ColumnChunkData> createNullChunkData(bool enableCompression,
        uint64_t capacity, ResidencyState type) {
        return std::make_unique<NullChunkData>(capacity, enableCompression, type);
    }
};

} // namespace storage
} // namespace kuzu
