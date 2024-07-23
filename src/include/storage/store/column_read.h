#pragma once

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/compression/compression_float.h"
#include "storage/db_file_id.h"

namespace kuzu {
namespace transaction {
class Transaction;
}
namespace storage {

class ColumnReader;
class ShadowFile;
struct ColumnChunkMetadata;
struct ChunkState;

template<typename OutputType>
using read_value_from_page_func_t = std::function<void(uint8_t*, PageCursor&, OutputType, uint32_t,
    uint64_t, const CompressionMetadata&)>;

template<typename OutputType>
using read_values_from_page_func_t = std::function<void(uint8_t*, PageCursor&, OutputType, uint32_t,
    uint64_t, const CompressionMetadata&)>;

using read_values_to_vector_func_t = read_values_from_page_func_t<common::ValueVector*>;
using read_values_to_page_func_t = read_values_from_page_func_t<uint8_t*>;

template<typename InputType, typename... AdditionalArgs>
using write_values_to_page_func_t = std::function<void(uint8_t*, uint16_t, InputType, uint32_t,
    common::offset_t, const CompressionMetadata&, AdditionalArgs...)>;

using write_values_from_vector_func_t = write_values_to_page_func_t<common::ValueVector*>;

using write_values_func_t = write_values_to_page_func_t<const uint8_t*, const common::NullMask*>;

using filter_func_t = std::function<bool(common::offset_t, common::offset_t)>;

struct ColumnReaderFactory {
    static std::unique_ptr<ColumnReader> createColumnReader(common::PhysicalTypeID dataType,
        DBFileID dbFileID, BMFileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile);
};

// In memory representation of ALP exception chunk
// NOTE: read and write operations on this chunk cannot both be performed on this
// After write operations are performed, you must call flushToDisk() before reading again
template<std::floating_point T>
class InMemoryExceptionChunk {
public:
    explicit InMemoryExceptionChunk(ColumnReader* columnReader,
        transaction::Transaction* transaction, const ChunkState& state);

    void finalizeAndFlushToDisk(ColumnReader* columnReader, ChunkState& state);

    void addException(EncodeException<T> exception);

    void removeExceptionAt(size_t exceptionIdx);

    EncodeException<T> getExceptionAt(size_t exceptionIdx) const;

    // TODO: can return a pair [idx, exception] so we don't need to duplicate read
    common::offset_t findFirstExceptionAtOrPastOffset(common::offset_t offsetInChunk) const;

    size_t getExceptionCount() const;

private:
    void writeException(EncodeException<T> exception, size_t exceptionIdx);

    static PageCursor getExceptionPageCursor(const ColumnChunkMetadata& metadata,
        PageCursor pageBaseCursor, size_t exceptionCapacity);

    size_t exceptionCount;
    size_t exceptionCapacity;

    std::unique_ptr<std::byte[]> exceptionBuffer;
    common::NullMask emptyMask;
};

class ColumnReader {
public:
    ColumnReader(DBFileID dbFileID, BMFileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile);

    virtual ~ColumnReader() = default;

    virtual void readCompressedValueToPage(transaction::Transaction* transaction,
        const ChunkState& state, common::offset_t nodeOffset, uint8_t* result,
        uint32_t offsetInResult, read_value_from_page_func_t<uint8_t*> readFunc) = 0;

    virtual void readCompressedValueToVector(transaction::Transaction* transaction,
        const ChunkState& state, common::offset_t nodeOffset, common::ValueVector* result,
        uint32_t offsetInResult, read_value_from_page_func_t<common::ValueVector*> readFunc) = 0;

    virtual uint64_t readCompressedValuesToPage(transaction::Transaction* transaction,
        const ChunkState& state, uint8_t* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<uint8_t*> readFunc,
        std::function<bool(common::offset_t, common::offset_t)> filterFunc) = 0;

    virtual uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ChunkState& state, common::ValueVector* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<common::ValueVector*> readFunc,
        std::function<bool(common::offset_t, common::offset_t)> filterFunc) = 0;

    virtual void writeValueToPageFromVector(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom,
        write_values_from_vector_func_t writeFromVectorFunc) = 0;

    virtual void writeValuesToPageFromBuffer(ChunkState& state, common::offset_t dstOffset,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset,
        common::offset_t numValues, write_values_func_t writeFunc) = 0;

protected:
    std::pair<common::offset_t, PageCursor> getOffsetAndCursor(common::offset_t nodeOffset,
        const ChunkState& state);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

    void updatePageWithCursor(PageCursor cursor,
        const std::function<void(uint8_t*, common::offset_t)>& writeOp) const;

    PageCursor getPageCursorForOffsetInGroup(common::offset_t offsetInChunk,
        common::page_idx_t groupPageIdx, uint64_t numValuesPerPage) const;

private:
    DBFileID dbFileID;
    BMFileHandle* dataFH;
    BufferManager* bufferManager;
    ShadowFile* shadowFile;

    friend InMemoryExceptionChunk<float>;
    friend InMemoryExceptionChunk<double>;
};

} // namespace storage
} // namespace kuzu
