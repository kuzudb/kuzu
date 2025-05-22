#pragma once

#include "storage/buffer_manager/memory_manager.h"
#include "storage/table/dictionary_column.h"

namespace kuzu {
namespace storage {

class StringColumn final : public Column {
public:
    enum class ChildStateIndex : common::idx_t { DATA = 0, OFFSET = 1, INDEX = 2 };
    static constexpr size_t CHILD_STATE_COUNT = 3;

    StringColumn(std::string name, common::LogicalType dataType, FileHandle* dataFH,
        MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression);

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunkData,
        FileHandle& dataFH);

    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) const override;
    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) const override;

    void write(ColumnChunkData& persistentChunk, ChunkState& state, common::offset_t dstOffset,
        ColumnChunkData* data, common::offset_t srcOffset, common::length_t numValues) override;

    void checkpointColumnChunk(ColumnCheckpointState& checkpointState) override;

    const DictionaryColumn& getDictionary() const { return dictionary; }
    const Column* getIndexColumn() const { return indexColumn.get(); }

    static ChunkState& getChildState(ChunkState& state, ChildStateIndex child);
    static const ChunkState& getChildState(const ChunkState& state, ChildStateIndex child);

protected:
    void scanInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector) const override;
    void scanUnfiltered(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::offset_t numValuesToRead,
        common::ValueVector* resultVector, common::sel_t startPosInVector = 0) const;
    void scanFiltered(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::ValueVector* resultVector) const;

    void lookupInternal(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector,
        uint32_t posInVector) const override;

private:
    bool canCheckpointInPlace(const ChunkState& state,
        const ColumnCheckpointState& checkpointState) override;

    bool canIndexCommitInPlace(const ChunkState& state, uint64_t numStrings,
        common::offset_t maxOffset) const;

private:
    // Main column stores indices of values in the dictionary
    DictionaryColumn dictionary;

    std::unique_ptr<Column> indexColumn;
};

} // namespace storage
} // namespace kuzu
