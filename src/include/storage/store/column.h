#pragma once

#include "catalog/catalog.h"
#include "common/null_mask.h"
#include "storage/stats/metadata_dah_info.h"
#include "storage/stats/property_statistics.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

struct CompressionMetadata;

using read_values_to_vector_func_t =
    std::function<void(uint8_t* frame, PageCursor& pageCursor, common::ValueVector* resultVector,
        uint32_t posInVector, uint32_t numValuesToRead, const CompressionMetadata& metadata)>;
using write_values_from_vector_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    common::ValueVector* vector, uint32_t posInVector, const CompressionMetadata& metadata)>;
using write_values_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    const uint8_t* data, common::offset_t dataOffset, common::offset_t numValues,
    const CompressionMetadata& metadata, const common::NullMask*)>;

using read_values_to_page_func_t =
    std::function<void(uint8_t* frame, PageCursor& pageCursor, uint8_t* result,
        uint32_t posInResult, uint64_t numValues, const CompressionMetadata& metadata)>;
// This is a special usage for the `batchLookup` interface.
using batch_lookup_func_t = read_values_to_page_func_t;

class NullColumn;
class StructColumn;
class RelTableData;
class Column {
    friend class StringColumn;
    friend class ListLocalColumn;
    friend class StructColumn;
    friend class ListColumn;
    friend class RelTableData;

public:
    struct ChunkState {
        explicit ChunkState() = default;
        ChunkState(ColumnChunkMetadata metadata, uint64_t numValuesPerPage)
            : metadata{std::move(metadata)}, numValuesPerPage{numValuesPerPage} {}

        ColumnChunkMetadata metadata;
        uint64_t numValuesPerPage = UINT64_MAX;
        common::node_group_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
        std::unique_ptr<ChunkState> nullState = nullptr;
        // Used for struct/list/string columns.
        std::vector<ChunkState> childrenStates;
    };

    Column(std::string name, common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
        bool enableCompression, bool requireNullColumn = true);
    virtual ~Column();

    // Expose for feature store
    virtual void batchLookup(transaction::Transaction* transaction,
        const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void initChunkState(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, ChunkState& state);

    virtual void scan(transaction::Transaction* transaction, ChunkState& state,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookup(transaction::Transaction* transaction, ChunkState& state,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);

    // Scan from [startOffsetInGroup, endOffsetInGroup).
    virtual void scan(transaction::Transaction* transaction, ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector);
    // Scan from [startOffsetInGroup, endOffsetInGroup).
    virtual void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET);

    // Append column chunk in a new node group.
    virtual void append(ColumnChunk* columnChunk, common::node_group_idx_t nodeGroupIdx);

    common::LogicalType& getDataType() { return dataType; }
    const common::LogicalType& getDataType() const { return dataType; }
    uint32_t getNumBytesPerValue() const { return numBytesPerFixedSizedValue; }
    uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return metadataDA->getNumElements(transaction->getType());
    }

    Column* getNullColumn();

    virtual void prepareCommit();
    void prepareCommitForChunk(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup,
        const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
        const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
        const offset_set_t& deleteInfo);
    void prepareCommitForChunk(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup,
        const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
        common::offset_t startSrcOffset);

    virtual void prepareCommitForExistingChunk(transaction::Transaction* transaction,
        ChunkState& state, const ChunkCollection& localInsertChunks,
        const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo);
    virtual void prepareCommitForExistingChunk(transaction::Transaction* transaction,
        ChunkState& state, const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
        common::offset_t startSrcOffset);

    virtual void checkpointInMemory();
    virtual void rollbackInMemory();

    void populateWithDefaultVal(transaction::Transaction* transaction,
        DiskArray<ColumnChunkMetadata>* metadataDA, common::ValueVector* defaultValueVector);

    ColumnChunkMetadata getMetadata(common::node_group_idx_t nodeGroupIdx,
        transaction::TransactionType transaction) const {
        return metadataDA->get(nodeGroupIdx, transaction);
    }
    DiskArray<ColumnChunkMetadata>* getMetadataDA() const { return metadataDA.get(); }

    std::string getName() const { return name; }

    virtual void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup, uint8_t* result);

    // Write a single value from the vectorToWriteFrom.
    virtual void write(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    // Batch write to a set of sequential pages.
    virtual void write(ChunkState& state, common::offset_t offsetInChunk, ColumnChunk* data,
        common::offset_t dataOffset, common::length_t numValues);

    // Append values to the end of the node group, resizing it if necessary
    common::offset_t appendValues(ChunkState& state, const uint8_t* data,
        const common::NullMask* nullChunkData, common::offset_t numValues);

    virtual std::unique_ptr<ColumnChunk> getEmptyChunkForCommit(uint64_t capacity);
    static void applyLocalChunkToColumnChunk(const ChunkCollection& localChunks,
        ColumnChunk* columnChunk, const offset_to_row_idx_t& info);

protected:
    virtual void scanInternal(transaction::Transaction* transaction, ChunkState& state,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    void scanUnfiltered(transaction::Transaction* transaction, PageCursor& pageCursor,
        uint64_t numValuesToScan, common::ValueVector* resultVector,
        const ColumnChunkMetadata& chunkMeta, uint64_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, PageCursor& pageCursor,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector,
        const ColumnChunkMetadata& chunkMeta);
    virtual void lookupInternal(transaction::Transaction* transaction, ChunkState& state,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupValue(transaction::Transaction* transaction, ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector, uint32_t posInVector);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

    virtual void writeValue(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    virtual void writeValues(ChunkState& state, common::offset_t offsetInChunk, const uint8_t* data,
        const common::NullMask* nullChunkData, common::offset_t dataOffset = 0,
        common::offset_t numValues = 1);

    // Produces a page cursor for the offset relative to the given node group
    PageCursor getPageCursorForOffsetInGroup(common::offset_t offsetInChunk,
        const ChunkState& state);
    void updatePageWithCursor(PageCursor cursor,
        const std::function<void(uint8_t*, common::offset_t)>& writeOp);

    common::offset_t getMaxOffset(const std::vector<common::offset_t>& offsets) {
        return offsets.empty() ? 0 : *std::max_element(offsets.begin(), offsets.end());
    }
    common::offset_t getMaxOffset(const offset_to_row_idx_t& offsets) {
        return offsets.empty() ? 0 : offsets.rbegin()->first;
    }

    static ChunkCollection getNullChunkCollection(const ChunkCollection& chunkCollection);

private:
    bool isInsertionsOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
        const offset_to_row_idx_t& insertInfo);
    bool isMaxOffsetOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
        common::offset_t maxOffset);
    bool checkUpdateInPlace(const ColumnChunkMetadata& metadata, const ChunkCollection& localChunks,
        const offset_to_row_idx_t& writeInfo);

    virtual bool canCommitInPlace(const ChunkState& state, const ChunkCollection& localInsertChunks,
        const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
        const offset_to_row_idx_t& updateInfo);
    virtual bool canCommitInPlace(const ChunkState& state,
        const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
        common::offset_t srcOffset);

    virtual void commitLocalChunkInPlace(ChunkState& state,
        const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
        const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
        const offset_set_t& deleteInfo);
    virtual void commitLocalChunkOutOfPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup,
        const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
        const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
        const offset_set_t& deleteInfo);

    virtual void commitColumnChunkInPlace(ChunkState& state,
        const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
        common::offset_t srcOffset);
    virtual void commitColumnChunkOutOfPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, bool isNewNodeGroup,
        const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
        common::offset_t srcOffset);

    void applyLocalChunkToColumn(ChunkState& state, const ChunkCollection& localChunks,
        const offset_to_row_idx_t& info);

    // check if val is in range [start, end)
    static bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
        return val >= start && val < end;
    }

protected:
    std::string name;
    DBFileID dbFileID;
    common::LogicalType dataType;
    // TODO(bmwinger): Remove. Only used by StorageDriver, which should be rewritten to not use this
    // field.
    uint32_t numBytesPerFixedSizedValue;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    BufferManager* bufferManager;
    WAL* wal;
    std::unique_ptr<DiskArray<ColumnChunkMetadata>> metadataDA;
    std::unique_ptr<NullColumn> nullColumn;
    read_values_to_vector_func_t readToVectorFunc;
    write_values_from_vector_func_t writeFromVectorFunc;
    write_values_func_t writeFunc;
    read_values_to_page_func_t readToPageFunc;
    batch_lookup_func_t batchLookupFunc;
    RWPropertyStats propertyStatistics;
    bool enableCompression;
};

class InternalIDColumn : public Column {
public:
    InternalIDColumn(std::string name, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats stats, bool enableCompression);

    void scan(transaction::Transaction* transaction, ChunkState& state,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override {
        Column::scan(transaction, state, nodeIDVector, resultVector);
        populateCommonTableID(resultVector);
    }

    void scan(transaction::Transaction* transaction, ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) override {
        Column::scan(transaction, state, startOffsetInGroup, endOffsetInGroup, resultVector,
            offsetInVector);
        populateCommonTableID(resultVector);
    }

    void lookup(transaction::Transaction* transaction, ChunkState& state,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override {
        Column::lookup(transaction, state, nodeIDVector, resultVector);
        populateCommonTableID(resultVector);
    }

    // TODO(Guodong): This function should be removed through rewritting INTERNAL_ID as STRUCT.
    void setCommonTableID(common::table_id_t tableID) { commonTableID = tableID; }

private:
    void populateCommonTableID(common::ValueVector* resultVector) const;

private:
    common::table_id_t commonTableID;
};

struct ColumnFactory {
    static std::unique_ptr<Column> createColumn(std::string name, common::LogicalType dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);
};

} // namespace storage
} // namespace kuzu
