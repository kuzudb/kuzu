#pragma once

#include "storage/compression/float_compression.h"
#include "storage/store/column_reader_writer.h"

namespace kuzu {
namespace transaction {
class Transaction;
}
namespace storage {

class ColumnReadWriter;
struct ColumnChunkMetadata;
struct ChunkState;

// In memory representation of ALP exception chunk
// NOTE: read and write operations on this chunk cannot both be performed on this
// Additionally, each exceptionIdx can be updated at most once before finalizing
// After such operations are performed, you must call finalizeAndFlushToDisk() before reading again
template<std::floating_point T>
class InMemoryExceptionChunk {
public:
    explicit InMemoryExceptionChunk(ColumnReadWriter* columnReader,
        transaction::Transaction* transaction, const ChunkState& state);

    void finalizeAndFlushToDisk(ColumnReadWriter* columnReader, ChunkState& state);

    void addException(EncodeException<T> exception);

    void removeExceptionAt(size_t exceptionIdx);

    EncodeException<T> getExceptionAt(size_t exceptionIdx) const;

    common::offset_t findFirstExceptionAtOrPastOffset(common::offset_t offsetInChunk) const;

    size_t getExceptionCount() const;

    void writeException(EncodeException<T> exception, size_t exceptionIdx);

private:
    static PageCursor getExceptionPageCursor(const ColumnChunkMetadata& metadata,
        PageCursor pageBaseCursor, size_t exceptionCapacity);

    void finalize(ChunkState& state);
    void flushToDisk(ColumnReadWriter* columnReader, ChunkState& state);

    size_t exceptionCount;
    size_t finalizedExceptionCount;
    size_t exceptionCapacity;

    std::unique_ptr<std::byte[]> exceptionBuffer;
    common::NullMask emptyMask;
};

} // namespace storage
} // namespace kuzu
