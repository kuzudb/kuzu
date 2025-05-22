#include "storage/table/null_column.h"

#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
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
            bool value = false;
            ConstantCompression::decompressValues(reinterpret_cast<uint8_t*>(&value), 0 /*offset*/,
                1 /*numValues*/, PhysicalTypeID::BOOL, 1 /*numBytesPerValue*/, metadata);
            resultVector->setNullRange(posInVector, numValuesToRead, value);
        } else {
            resultVector->setNullFromBits(reinterpret_cast<const uint64_t*>(frame),
                pageCursor.elemPosInPage, posInVector, numValuesToRead);
        }
    }
};

NullColumn::NullColumn(const std::string& name, FileHandle* dataFH, MemoryManager* mm,
    ShadowFile* shadowFile, bool enableCompression)
    : Column{name, LogicalType::BOOL(), dataFH, mm, shadowFile, enableCompression,
          false /*requireNullColumn*/} {
    readToVectorFunc = NullColumnFunc::readValuesFromPageToVector;
}

void NullColumn::scan(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* resultVector) const {
    scanInternal(transaction, state, startOffsetInChunk, numValuesToScan, resultVector);
}

void NullColumn::scan(const Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) const {
    Column::scan(transaction, state, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
}

void NullColumn::scan(const Transaction* transaction, const ChunkState& state,
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) const {
    Column::scan(transaction, state, columnChunk, startOffset, endOffset);
}

void NullColumn::write(ColumnChunkData& persistentChunk, ChunkState& state, offset_t offsetInChunk,
    ColumnChunkData* data, offset_t dataOffset, length_t numValues) {
    if (numValues == 0) {
        return;
    }
    writeValues(state, offsetInChunk, data->getData(), nullptr /*nullChunkData*/, dataOffset,
        numValues);
    const auto& nullChunk = data->cast<NullChunkData>();
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
