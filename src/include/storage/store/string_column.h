#pragma once

#include "storage/store/dictionary_column.h"

namespace kuzu {
namespace storage {

class StringColumn final : public Column {
public:
    enum class ChildStateIndex : common::idx_t { DATA = 0, OFFSET = 1, INDEX = 2 };
    static constexpr size_t CHILD_STATE_COUNT = 3;

    StringColumn(std::string name, common::LogicalType dataType, BMFileHandle* dataFH,
        BufferManager* bufferManager, ShadowFile* shadowFile, bool enableCompression);

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunkData,
        BMFileHandle& dataFH);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void write(ColumnChunkData& persistentChunk, ChunkState& state, common::offset_t dstOffset,
        ColumnChunkData* data, common::offset_t srcOffset, common::length_t numValues) override;

    void checkpointColumnChunk(kuzu::storage::ColumnCheckpointState& checkpointState) override;

    const DictionaryColumn& getDictionary() const { return dictionary; }

    static ChunkState& getChildState(ChunkState& state, ChildStateIndex child);
    static const ChunkState& getChildState(const ChunkState& state, ChildStateIndex child);

protected:
    void scanInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;
    void scanUnfiltered(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::offset_t numValuesToRead,
        common::ValueVector* resultVector, common::sel_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, const common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void lookupInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector,
        uint32_t posInVector) override;

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
