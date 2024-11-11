#pragma once

#include <functional>
#include <variant>

#include "common/constants.h"
#include "common/data_chunk/sel_vector.h"
#include "common/enums/rel_multiplicity.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/compression/compression.h"
#include "storage/enums/residency_state.h"
#include "storage/store/column_chunk_metadata.h"
#include "storage/store/column_chunk_stats.h"
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
    const Column* column;
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
class Spiller;
// Base data segment covers all fixed-sized data types.
class ColumnChunkData {
public:
    friend struct ColumnChunkFactory;
    // For spilling to disk we need access to the underlying buffer
    friend class Spiller;

    ColumnChunkData(MemoryManager& mm, common::LogicalType dataType, uint64_t capacity,
        bool enableCompression, ResidencyState residencyState, bool hasNullData,
        bool initializeToZero = true);
    ColumnChunkData(MemoryManager& mm, common::LogicalType dataType, bool enableCompression,
        const ColumnChunkMetadata& metadata, bool hasNullData, bool initializeToZero = true);
    ColumnChunkData(MemoryManager& mm, common::PhysicalTypeID physicalType, bool enableCompression,
        const ColumnChunkMetadata& metadata, bool hasNullData, bool initializeToZero = true);
    virtual ~ColumnChunkData();

    template<typename T>
    T getValue(common::offset_t pos) const {
        KU_ASSERT(pos < numValues);
        KU_ASSERT(residencyState != ResidencyState::ON_DISK);
        return getData<T>()[pos];
    }
    template<typename T>
    void setValue(T val, common::offset_t pos) {
        KU_ASSERT(pos < capacity);
        KU_ASSERT(residencyState != ResidencyState::ON_DISK);
        getData<T>()[pos] = val;
        if (pos >= numValues) {
            numValues = pos + 1;
        }
        if constexpr (StorageValueType<T>) {
            inMemoryStats.update(StorageValue{val}, dataType.getPhysicalType());
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
        return (numBytes + common::KUZU_PAGE_SIZE - 1) / common::KUZU_PAGE_SIZE;
    }

    uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    uint8_t* getData() const;
    template<typename T>
    T* getData() const {
        return reinterpret_cast<T*>(getData());
    }
    uint64_t getBufferSize() const;

    virtual void initializeScanState(ChunkState& state, const Column* column) const;
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

    virtual void setToInMemory();
    // numValues must be at least the number of values the ColumnChunk was first initialized
    // with
    // reverse data and zero the part exceeding the original size
    virtual void resize(uint64_t newCapacity);
    // the opposite of the resize method, just simple resize
    virtual void resizeWithoutPreserve(uint64_t newCapacity);

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
    static std::unique_ptr<ColumnChunkData> deserialize(MemoryManager& mm,
        common::Deserializer& deSer);

    template<typename TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<typename TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
    MemoryManager& getMemoryManager() const;

    void loadFromDisk();
    uint64_t spillToDisk();

    ColumnChunkStats getMergedColumnChunkStats() const;

    void updateStats(const common::ValueVector* vector, const common::SelectionVector& selVector);

protected:
    // Initializes the data buffer and functions. They are (and should be) only called in
    // constructor.
    void initializeBuffer(common::PhysicalTypeID physicalType, MemoryManager& mm,
        bool initializeToZero);
    void initializeFunction();

    // Note: This function is not setting child/null chunk data recursively.
    void setToOnDisk(const ColumnChunkMetadata& metadata);

    virtual void copyVectorToBuffer(common::ValueVector* vector, common::offset_t startPosInChunk,
        const common::SelectionVector& selVector);

    void resetInMemoryStats();

private:
    using flush_buffer_func_t = std::function<ColumnChunkMetadata(const std::span<uint8_t>,
        FileHandle*, common::page_idx_t, const ColumnChunkMetadata&)>;
    flush_buffer_func_t initializeFlushBufferFunction(
        std::shared_ptr<CompressionAlg> compression) const;
    uint64_t getBufferSize(uint64_t capacity_) const;

protected:
    using get_metadata_func_t = std::function<ColumnChunkMetadata(const std::span<uint8_t>,
        uint64_t, uint64_t, StorageValue, StorageValue)>;
    using get_min_max_func_t =
        std::function<std::pair<StorageValue, StorageValue>(const uint8_t*, uint64_t)>;

    ResidencyState residencyState;
    common::LogicalType dataType;
    bool enableCompression;
    uint32_t numBytesPerValue;
    uint64_t capacity;
    std::unique_ptr<MemoryBuffer> buffer;
    std::unique_ptr<NullChunkData> nullData;
    uint64_t numValues;
    flush_buffer_func_t flushBufferFunction;
    get_metadata_func_t getMetadataFunction;

    // On-disk metadata for column chunk.
    ColumnChunkMetadata metadata;

    // Stats for any in-memory updates applied to the column chunk
    // This will be merged with the on-disk metadata to get the overall stats
    ColumnChunkStats inMemoryStats;
};

template<>
inline void ColumnChunkData::setValue(bool val, common::offset_t pos) {
    // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
    common::NullMask::setNull(getData<uint64_t>(), pos, val);
}

template<>
inline bool ColumnChunkData::getValue(common::offset_t pos) const {
    // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
    return common::NullMask::isNull(getData<uint64_t>(), pos);
}

// Stored as bitpacked booleans in-memory and on-disk
class BoolChunkData : public ColumnChunkData {
public:
    BoolChunkData(MemoryManager& mm, uint64_t capacity, bool enableCompression, ResidencyState type,
        bool hasNullChunk)
        : ColumnChunkData(mm, common::LogicalType::BOOL(), capacity,
              // Booleans are always bitpacked, but this can also enable constant compression
              enableCompression, type, hasNullChunk, true) {}
    BoolChunkData(MemoryManager& mm, bool enableCompression, const ColumnChunkMetadata& metadata,
        bool hasNullData)
        : ColumnChunkData{mm, common::LogicalType::BOOL(), enableCompression, metadata, hasNullData,
              true} {}

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
    NullChunkData(MemoryManager& mm, uint64_t capacity, bool enableCompression, ResidencyState type)
        : BoolChunkData(mm, capacity, enableCompression, type, false /*hasNullData*/),
          mayHaveNullValue{false} {}
    NullChunkData(MemoryManager& mm, bool enableCompression, const ColumnChunkMetadata& metadata)
        : BoolChunkData{mm, enableCompression, metadata, false /*hasNullData*/},
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
        memset(getData(), 0 /* non null */, getBufferSize());
        mayHaveNullValue = false;
    }
    void resetToAllNull() override {
        memset(getData(), 0xFF /* null */, getBufferSize());
        mayHaveNullValue = true;
    }

    void copyFromBuffer(uint64_t* srcBuffer, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBits, bool invert = false) {
        if (common::NullMask::copyNullMask(srcBuffer, srcOffset, getData<uint64_t>(), dstOffset,
                numBits, invert)) {
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
    static std::unique_ptr<NullChunkData> deserialize(MemoryManager& mm,
        common::Deserializer& deSer);

    common::NullMask getNullMask() const {
        return common::NullMask(std::span(getData<uint64_t>(), capacity / 64), mayHaveNullValue);
    }

protected:
    bool mayHaveNullValue;
};

class InternalIDChunkData final : public ColumnChunkData {
public:
    // TODO(Guodong): Should make InternalIDChunkData has no NULL.
    // Physically, we only materialize offset of INTERNAL_ID, which is same as UINT64,
    InternalIDChunkData(MemoryManager& mm, uint64_t capacity, bool enableCompression,
        ResidencyState residencyState)
        : ColumnChunkData(mm, common::LogicalType::INTERNAL_ID(), capacity, enableCompression,
              residencyState, false /*hasNullData*/),
          commonTableID{common::INVALID_TABLE_ID} {}
    InternalIDChunkData(MemoryManager& mm, bool enableCompression,
        const ColumnChunkMetadata& metadata)
        : ColumnChunkData{mm, common::LogicalType::INTERNAL_ID(), enableCompression, metadata,
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
    common::offset_t& operator[](common::offset_t pos) { return getData<common::offset_t>()[pos]; }

private:
    common::table_id_t commonTableID;
};

struct ColumnChunkFactory {
    static std::unique_ptr<ColumnChunkData> createColumnChunkData(MemoryManager& mm,
        common::LogicalType dataType, bool enableCompression, uint64_t capacity,
        ResidencyState residencyState, bool hasNullData = true, bool initializeToZero = true);
    static std::unique_ptr<ColumnChunkData> createColumnChunkData(MemoryManager& mm,
        common::LogicalType dataType, bool enableCompression, ColumnChunkMetadata& metadata,
        bool hasNullData, bool initializeToZero);

    static std::unique_ptr<ColumnChunkData> createNullChunkData(MemoryManager& mm,
        bool enableCompression, uint64_t capacity, ResidencyState type) {
        return std::make_unique<NullChunkData>(mm, capacity, enableCompression, type);
    }
};

} // namespace storage
} // namespace kuzu
