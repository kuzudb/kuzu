#pragma once

#include "catalog/catalog.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "storage/table/column_chunk_data.h"
#include "storage/table/column_reader_writer.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator
namespace storage {
class MemoryManager;

struct CompressionMetadata;

// This is a special usage for the `batchLookup` interface.
using batch_lookup_func_t = read_values_to_page_func_t;

class NullColumn;
class StructColumn;
class RelTableData;
struct ColumnCheckpointState;
class BufferManager;
class Column {
    friend class StringColumn;
    friend class StructColumn;
    friend class ListColumn;
    friend class RelTableData;

public:
    // TODO(Guodong): Remove transaction from interface of Column. There is no need to be aware
    // of transaction when reading/writing from/to disk pages.
    Column(std::string name, common::LogicalType dataType, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression, bool requireNullColumn = true);
    Column(std::string name, common::PhysicalTypeID physicalType, FileHandle* dataFH,
        MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression,
        bool requireNullColumn = true);

    virtual ~Column();

    void populateExtraChunkState(ChunkState& state) const;

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunkData,
        FileHandle& dataFH);
    static std::unique_ptr<ColumnChunkData> flushNonNestedChunkData(
        const ColumnChunkData& chunkData, FileHandle& dataFH);
    static ColumnChunkMetadata flushData(const ColumnChunkData& chunkData, FileHandle& dataFH);

    virtual void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector) const;
    virtual void lookupValue(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector, uint32_t posInVector) const;

    // Scan from [startOffsetInGroup, endOffsetInGroup).
    virtual void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) const;
    // Scan from [startOffsetInGroup, endOffsetInGroup).
    virtual void scan(const transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) const;

    common::LogicalType& getDataType() { return dataType; }
    const common::LogicalType& getDataType() const { return dataType; }

    Column* getNullColumn() const;

    std::string getName() const { return name; }

    virtual void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup, uint8_t* result);

    // Batch write to a set of sequential pages.
    virtual void write(ColumnChunkData& persistentChunk, ChunkState& state,
        common::offset_t dstOffset, ColumnChunkData* data, common::offset_t srcOffset,
        common::length_t numValues);

    // Append values to the end of the node group, resizing it if necessary
    common::offset_t appendValues(ColumnChunkData& persistentChunk, ChunkState& state,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t numValues);

    virtual void checkpointColumnChunk(ColumnCheckpointState& checkpointState);

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }

protected:
    virtual void scanInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector) const;

    virtual void lookupInternal(const transaction::Transaction* transaction,
        const ChunkState& state, common::offset_t nodeOffset, common::ValueVector* resultVector,
        uint32_t posInVector) const;

    common::page_idx_t writeValues(ChunkState& state, common::offset_t dstOffset,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset = 0,
        common::offset_t numValues = 1);

    // Produces a page cursor for the offset relative to the given node group
    static PageCursor getPageCursorForOffsetInGroup(common::offset_t offsetInChunk,
        const ChunkState& state);
    void updatePageWithCursor(PageCursor cursor,
        const std::function<void(uint8_t*, common::offset_t)>& writeOp) const;

    void updateStatistics(ColumnChunkMetadata& metadata, common::offset_t maxIndex,
        const std::optional<StorageValue>& min, const std::optional<StorageValue>& max) const;

protected:
    bool isEndOffsetOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
        common::offset_t maxOffset) const;

    virtual bool canCheckpointInPlace(const ChunkState& state,
        const ColumnCheckpointState& checkpointState);

    void checkpointColumnChunkInPlace(ChunkState& state,
        const ColumnCheckpointState& checkpointState);
    void checkpointNullData(const ColumnCheckpointState& checkpointState) const;

    void checkpointColumnChunkOutOfPlace(const ChunkState& state,
        const ColumnCheckpointState& checkpointState);

    // check if val is in range [start, end)
    static bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
        return val >= start && val < end;
    }

protected:
    std::string name;
    common::LogicalType dataType;
    FileHandle* dataFH;
    MemoryManager* mm;
    ShadowFile* shadowFile;
    std::unique_ptr<NullColumn> nullColumn;
    read_values_to_vector_func_t readToVectorFunc;
    write_values_func_t writeFunc;
    read_values_to_page_func_t readToPageFunc;
    bool enableCompression;

    std::unique_ptr<ColumnReadWriter> columnReadWriter;
};

class InternalIDColumn final : public Column {
public:
    InternalIDColumn(std::string name, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector) const override {
        Column::scan(transaction, state, startOffsetInChunk, numValuesToScan, resultVector);
        populateCommonTableID(resultVector);
    }

    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) const override {
        Column::scan(transaction, state, startOffsetInGroup, endOffsetInGroup, resultVector,
            offsetInVector);
        populateCommonTableID(resultVector);
    }

    void lookupInternal(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector,
        uint32_t posInVector) const override {
        Column::lookupInternal(transaction, state, nodeOffset, resultVector, posInVector);
        populateCommonTableID(resultVector);
    }

    common::table_id_t getCommonTableID() const { return commonTableID; }
    // TODO(Guodong): This function should be removed through rewritting INTERNAL_ID as STRUCT.
    void setCommonTableID(common::table_id_t tableID) { commonTableID = tableID; }

private:
    void populateCommonTableID(const common::ValueVector* resultVector) const;

private:
    common::table_id_t commonTableID;
};

struct ColumnFactory {
    static std::unique_ptr<Column> createColumn(std::string name, common::LogicalType dataType,
        FileHandle* dataFH, MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression);
    static std::unique_ptr<Column> createColumn(std::string name,
        common::PhysicalTypeID physicalType, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression);
};

} // namespace storage
} // namespace kuzu
