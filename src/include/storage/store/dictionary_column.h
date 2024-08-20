#pragma once

#include "dictionary_chunk.h"
#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class DictionaryColumn {
public:
    DictionaryColumn(const std::string& name, BMFileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile, bool enableCompression);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        DictionaryChunk& dictChunk) const;
    // Offsets to scan should be a sorted list of pairs mapping the index of the entry in the string
    // dictionary (as read from the index column) to the output index in the result vector to store
    // the string.
    void scan(transaction::Transaction* transaction, const ChunkState& offsetState,
        const ChunkState& dataState,
        std::vector<std::pair<DictionaryChunk::string_index_t, uint64_t>>& offsetsToScan,
        common::ValueVector* resultVector, const ColumnChunkMetadata& indexMeta);

    DictionaryChunk::string_index_t append(const DictionaryChunk& dictChunk,
        const ChunkState& state, std::string_view val);

    bool canCommitInPlace(const ChunkState& state, uint64_t numNewStrings,
        uint64_t totalStringLengthToAdd);

    Column* getDataColumn() const { return dataColumn.get(); }
    Column* getOffsetColumn() const { return offsetColumn.get(); }

private:
    void scanOffsets(transaction::Transaction* transaction, const ChunkState& state,
        DictionaryChunk::string_offset_t* offsets, uint64_t index, uint64_t numValues,
        uint64_t dataSize);
    void scanValueToVector(transaction::Transaction* transaction, const ChunkState& dataState,
        uint64_t startOffset, uint64_t endOffset, common::ValueVector* resultVector,
        uint64_t offsetInVector);

    bool canDataCommitInPlace(const ChunkState& dataState, uint64_t totalStringLengthToAdd);
    bool canOffsetCommitInPlace(const ChunkState& offsetState, const ChunkState& dataState,
        uint64_t numNewStrings, uint64_t totalStringLengthToAdd);

private:
    // The offset column stores the offsets for each index, and the data column stores the data in
    // order. Values are never removed from the dictionary during in-place updates, only appended to
    // the end.
    std::unique_ptr<Column> dataColumn;
    std::unique_ptr<Column> offsetColumn;
};

} // namespace storage
} // namespace kuzu
