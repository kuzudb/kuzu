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

NullColumn::NullColumn(std::string name, BMFileHandle* dataFH, BufferManager* bufferManager,
    ShadowFile* shadowFile, bool enableCompression)
    : Column{name, LogicalType::BOOL(), dataFH, bufferManager, shadowFile, enableCompression,
          false /*requireNullColumn*/} {
    readToVectorFunc = NullColumnFunc::readValuesFromPageToVector;
    writeFromVectorFunc = NullColumnFunc::writeValueToPageFromVector;
    // Should never be used
    batchLookupFunc = nullptr;
}

void NullColumn::scan(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    scanInternal(transaction, state, startOffsetInChunk, numValuesToScan, nodeIDVector,
        resultVector);
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

void NullColumn::setNull(ColumnChunkData& persistentChunk, ChunkState& state,
    offset_t offsetInChunk, uint64_t value) {
    // Must be aligned to an 8-byte chunk for NullMask read to not overflow
    writeValues(persistentChunk, state, offsetInChunk, reinterpret_cast<const uint8_t*>(&value),
        nullptr /*nullChunkData=*/);
    updateStatistics(persistentChunk.getMetadata(), offsetInChunk, StorageValue((bool)value),
        StorageValue((bool)value));
}

void NullColumn::write(ColumnChunkData& persistentChunk, ChunkState& state, offset_t offsetInChunk,
    ColumnChunkData* data, offset_t dataOffset, length_t numValues) {
    if (numValues == 0) {
        return;
    }
    writeValues(persistentChunk, state, offsetInChunk, data->getData(), nullptr /*nullChunkData*/,
        dataOffset, numValues);
    auto& nullChunk = data->cast<NullChunkData>();
    bool min = nullChunk.isNull(dataOffset);
    bool max = min;
    for (auto i = 0u; i < numValues; i++) {
        if (nullChunk.isNull(dataOffset + i)) {
            max = true;
        } else {
            min = false;
        }
    }
    updateStatistics(persistentChunk.getMetadata(), offsetInChunk + numValues - 1,
        StorageValue(min), StorageValue(max));
}

} // namespace storage
} // namespace kuzu
