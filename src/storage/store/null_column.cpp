#include "storage/store/null_column.h"

#include "storage/compression/compression.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

struct NullColumnFunc {
    static void readValuesFromPageToVector(const uint8_t* frame, PageCursor& pageCursor,
        ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
        const CompressionMetadata& metadata) {
        // Read bit-packed null flags from the frame into the result vector
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        if (metadata.isConstant()) {
            bool value;
            ConstantCompression::decompressValues(reinterpret_cast<uint8_t*>(&value), 0 /*offset*/,
                1 /*numValues*/, PhysicalTypeID::BOOL, 1 /*numBytesPerValue*/, metadata);
            resultVector->setNullRange(posInVector, numValuesToRead, value);
        } else {
            resultVector->setNullFromBits((uint64_t*)frame, pageCursor.elemPosInPage, posInVector,
                numValuesToRead);
        }
    }

    static void writeValueToPageFromVector(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& metadata) {
        if (metadata.isConstant()) {
            // Value to write is identical to the constant value
            return;
        }
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        NullMask::setNull((uint64_t*)frame, posInFrame, vector->isNull(posInVector));
    }
};

NullColumn::NullColumn(std::string name, page_idx_t metaDAHIdx, BMFileHandle* dataFH,
    DiskArrayCollection& metadataDAC, BufferManager* bufferManager, WAL* wal,
    Transaction* transaction, bool enableCompression)
    : Column{name, LogicalType::BOOL(), MetadataDAHInfo{metaDAHIdx}, dataFH, metadataDAC,
          bufferManager, wal, transaction, enableCompression, false /*requireNullColumn*/} {
    readToVectorFunc = NullColumnFunc::readValuesFromPageToVector;
    writeFromVectorFunc = NullColumnFunc::writeValueToPageFromVector;
    // Should never be used
    batchLookupFunc = nullptr;
}

void NullColumn::scan(Transaction* transaction, const ChunkState& state, idx_t vectorIdx,
    row_idx_t numValuesToScan, ValueVector* nodeIDVector, ValueVector* resultVector) {
    scanInternal(transaction, state, vectorIdx, numValuesToScan, nodeIDVector, resultVector);
}

void NullColumn::scan(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    Column::scan(transaction, state, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
}

void NullColumn::scan(Transaction* transaction, const ChunkState& state,
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) {
    Column::scan(transaction, state, columnChunk, startOffset, endOffset);
}

void NullColumn::lookup(Transaction* transaction, ChunkState& readState, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    lookupInternal(transaction, readState, nodeIDVector, resultVector);
}

void NullColumn::append(ColumnChunkData* columnChunk, ChunkState& state) {
    auto preScanMetadata = columnChunk->getMetadataToFlush();
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    state.metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
    metadataDA->resize(state.nodeGroupIdx + 1);
    metadataDA->update(state.nodeGroupIdx, state.metadata);
}

bool NullColumn::isNull(Transaction* transaction, const ChunkState& state, offset_t offsetInChunk) {
    uint64_t result = false;
    if (offsetInChunk >= state.metadata.numValues) {
        return true;
    }
    // Must be aligned to an 8-byte chunk for NullMask read to not overflow
    Column::scan(transaction, state, offsetInChunk, offsetInChunk + 1,
        reinterpret_cast<uint8_t*>(&result));
    return result;
}

void NullColumn::setNull(ChunkState& state, offset_t offsetInChunk, uint64_t value) {
    // Must be aligned to an 8-byte chunk for NullMask read to not overflow
    writeValues(state, offsetInChunk, reinterpret_cast<const uint8_t*>(&value),
        nullptr /*nullChunkData=*/);
    updateStatistics(state.metadata, offsetInChunk, StorageValue((bool)value),
        StorageValue((bool)value));
}

void NullColumn::write(ChunkState& state, offset_t offsetInChunk, ValueVector* vectorToWriteFrom,
    uint32_t posInVectorToWriteFrom) {
    writeValue(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom);
    auto value = StorageValue(vectorToWriteFrom->isNull(posInVectorToWriteFrom));
    updateStatistics(state.metadata, offsetInChunk, value, value);
}

void NullColumn::write(ChunkState& state, offset_t offsetInChunk, ColumnChunkData* data,
    offset_t dataOffset, length_t numValues) {
    writeValues(state, offsetInChunk, data->getData(), nullptr /*nullChunkData*/, dataOffset,
        numValues);
    auto& nullChunk = data->cast<NullChunkData>();
    KU_ASSERT(numValues > 0);
    bool min = nullChunk.isNull(dataOffset);
    bool max = min;
    for (auto i = 0u; i < numValues; i++) {
        if (nullChunk.isNull(dataOffset + i)) {
            max = true;
        } else {
            min = false;
        }
    }
    updateStatistics(state.metadata, offsetInChunk + numValues - 1, StorageValue(min),
        StorageValue(max));
}

void NullColumn::commitLocalChunkInPlace(ChunkState& state,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    for (auto& [offsetInChunk, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        auto localChunk = localUpdateChunks[chunkIdx];
        KU_ASSERT(localChunk->getDataType().getPhysicalType() == PhysicalTypeID::BOOL &&
                  !localChunk->getNullChunk());
        write(state, offsetInChunk, localChunk, offsetInLocalChunk, 1 /*numValues*/);
    }
    for (auto& [offsetInChunk, rowIdx] : insertInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        auto localChunk = localInsertChunks[chunkIdx];
        KU_ASSERT(localChunk->getDataType().getPhysicalType() == PhysicalTypeID::BOOL &&
                  !localChunk->getNullChunk());
        write(state, offsetInChunk, localChunk, offsetInLocalChunk, 1 /*numValues*/);
    }
    // Set nulls based on deleteInfo. Note that this code path actually only gets executed when
    // the column is a regular format one. This is not a good design, should be unified with csr
    // one in the future.
    for (auto offsetInChunk : deleteInfo) {
        setNull(state, offsetInChunk);
    }
}

} // namespace storage
} // namespace kuzu
