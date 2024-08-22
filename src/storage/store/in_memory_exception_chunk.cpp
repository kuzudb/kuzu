#include "storage/store/in_memory_exception_chunk.h"

#include "storage/compression/float_compression.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"
#include <concepts>

namespace kuzu::storage {

using namespace common;
using namespace transaction;

template<std::floating_point T>
InMemoryExceptionChunk<T>::InMemoryExceptionChunk(ColumnReadWriter* columnReader,
    Transaction* transaction, const ChunkState& state)
    : exceptionCount(state.metadata.compMeta.floatMetadata()->exceptionCount),
      finalizedExceptionCount(exceptionCount),
      exceptionCapacity(state.metadata.compMeta.floatMetadata()->exceptionCapacity),
      exceptionBuffer(
          std::make_unique<std::byte[]>(exceptionCapacity * EncodeException<T>::sizeInBytes())),
      emptyMask(exceptionCapacity) {
    // Read the ALP exceptions on disk into the in-memory exception buffer

    emptyMask.setNullFromRange(exceptionCount, exceptionCapacity - exceptionCount, true);

    auto exceptionCursor = getExceptionPageCursor(state.metadata,
        columnReader->getPageCursorForOffsetInGroup(0, state.metadata.pageIdx,
            state.numValuesPerPage),
        exceptionCapacity);
    KU_ASSERT(exceptionCursor.elemPosInPage == 0);

    const size_t numPagesToCopy = EncodeException<T>::numPagesFromExceptions(getExceptionCount());
    size_t remainingBytesToCopy = getExceptionCount() * EncodeException<T>::sizeInBytes();
    for (size_t i = 0; i < numPagesToCopy; ++i) {
        columnReader->readFromPage(transaction, exceptionCursor.pageIdx,
            [this, i, remainingBytesToCopy](uint8_t* frame) {
                std::memcpy(exceptionBuffer.get() + i * EncodeException<T>::exceptionBytesPerPage(),
                    frame,
                    std::min(EncodeException<T>::exceptionBytesPerPage(), remainingBytesToCopy));
            });

        remainingBytesToCopy -= EncodeException<T>::exceptionBytesPerPage();
        KU_ASSERT(remainingBytesToCopy >= 0);
        exceptionCursor.nextPage();
    }
}

template<std::floating_point T>
void InMemoryExceptionChunk<T>::finalizeAndFlushToDisk(ColumnReadWriter* columnReader,
    ChunkState& state) {
    finalize(state);
    flushToDisk(columnReader, state);
}

template<std::floating_point T>
void InMemoryExceptionChunk<T>::finalize(ChunkState& state) {
    // removes holes + sorts exception chunk
    finalizedExceptionCount = 0;
    for (size_t i = 0; i < exceptionCount; ++i) {
        if (!emptyMask.isNull(i)) {
            ++finalizedExceptionCount;
            if (finalizedExceptionCount - 1 == i) {
                continue;
            }
            writeException(getExceptionAt(i), finalizedExceptionCount - 1);
        }
    }

    KU_ASSERT(
        finalizedExceptionCount <= state.metadata.compMeta.floatMetadata()->exceptionCapacity);
    state.metadata.compMeta.floatMetadata()->exceptionCount = finalizedExceptionCount;

    using ExceptionWord = std::array<std::byte, EncodeException<T>::sizeInBytes()>;
    ExceptionWord* exceptionWordBuffer = reinterpret_cast<ExceptionWord*>(exceptionBuffer.get());
    std::sort(exceptionWordBuffer, exceptionWordBuffer + finalizedExceptionCount,
        [](ExceptionWord& a, ExceptionWord& b) {
            return EncodeExceptionView<T>{reinterpret_cast<std::byte*>(&a)}.getValue() <
                   EncodeExceptionView<T>{reinterpret_cast<std::byte*>(&b)}.getValue();
        });
    std::memset(exceptionBuffer.get() + finalizedExceptionCount * EncodeException<T>::sizeInBytes(),
        0, (exceptionCount - finalizedExceptionCount) * EncodeException<T>::sizeInBytes());
    emptyMask.setNullFromRange(0, finalizedExceptionCount, false);
    emptyMask.setNullFromRange(finalizedExceptionCount, (exceptionCount - finalizedExceptionCount),
        true);
    exceptionCount = finalizedExceptionCount;
}

template<std::floating_point T>
void InMemoryExceptionChunk<T>::flushToDisk(ColumnReadWriter* columnReader, ChunkState& state) {
    auto exceptionCursor = getExceptionPageCursor(state.metadata,
        columnReader->getPageCursorForOffsetInGroup(0, state.metadata.pageIdx,
            state.numValuesPerPage),
        exceptionCapacity);

    const size_t numPagesToFlush = EncodeException<T>::numPagesFromExceptions(exceptionCapacity);
    size_t remainingBytesToCopy = exceptionCapacity * EncodeException<T>::sizeInBytes();
    for (size_t i = 0; i < numPagesToFlush; ++i) {
        KU_ASSERT(exceptionCursor.elemPosInPage == 0);
        columnReader->updatePageWithCursor(exceptionCursor,
            [this, i, remainingBytesToCopy](auto frame, auto /*offsetInPage*/) {
                std::memcpy(frame,
                    exceptionBuffer.get() + i * EncodeException<T>::exceptionBytesPerPage(),
                    std::min(EncodeException<T>::exceptionBytesPerPage(), remainingBytesToCopy));
            });

        remainingBytesToCopy -= EncodeException<T>::exceptionBytesPerPage();
        KU_ASSERT(remainingBytesToCopy >= 0);
        exceptionCursor.nextPage();
    }
}

template<std::floating_point T>
void InMemoryExceptionChunk<T>::addException(EncodeException<T> exception) {
    KU_ASSERT(exceptionCount < exceptionCapacity);
    ++exceptionCount;
    writeException(exception, exceptionCount - 1);
    emptyMask.setNull(exceptionCount - 1, false);
}

template<std::floating_point T>
void InMemoryExceptionChunk<T>::removeExceptionAt(size_t exceptionIdx) {
    // removing an exception does not free up space in the exception buffer
    emptyMask.setNull(exceptionIdx, true);
}

template<std::floating_point T>
EncodeException<T> InMemoryExceptionChunk<T>::getExceptionAt(size_t exceptionIdx) const {
    KU_ASSERT(exceptionIdx < exceptionCount);
    return EncodeExceptionView<T>{exceptionBuffer.get()}.getValue(exceptionIdx);
}

template<std::floating_point T>
void InMemoryExceptionChunk<T>::writeException(EncodeException<T> exception, size_t exceptionIdx) {
    KU_ASSERT(exceptionIdx < exceptionCount);
    EncodeExceptionView<T>{exceptionBuffer.get()}.setValue(exception, exceptionIdx);
}

template<std::floating_point T>
offset_t InMemoryExceptionChunk<T>::findFirstExceptionAtOrPastOffset(offset_t offsetInChunk) const {
    // binary search for chunkOffset in exceptions
    // we only search among non-finalized exceptions

    offset_t lo = 0;
    offset_t hi = finalizedExceptionCount;
    while (lo < hi) {
        const size_t curExceptionIdx = (lo + hi) / 2;
        EncodeException<T> lastException = getExceptionAt(curExceptionIdx);

        if (lastException.posInChunk < offsetInChunk) {
            lo = curExceptionIdx + 1;
        } else {
            hi = curExceptionIdx;
        }
    }

    return lo;
}

template<std::floating_point T>
PageCursor InMemoryExceptionChunk<T>::getExceptionPageCursor(const ColumnChunkMetadata& metadata,
    PageCursor pageBaseCursor, size_t exceptionCapacity) {
    const size_t numExceptionPages = EncodeException<T>::numPagesFromExceptions(exceptionCapacity);
    const size_t exceptionPageOffset = metadata.numPages - numExceptionPages;
    KU_ASSERT(exceptionPageOffset == (page_idx_t)exceptionPageOffset);
    return {pageBaseCursor.pageIdx + (page_idx_t)exceptionPageOffset, 0};
}

template<std::floating_point T>
size_t InMemoryExceptionChunk<T>::getExceptionCount() const {
    return finalizedExceptionCount;
}

template class InMemoryExceptionChunk<float>;
template class InMemoryExceptionChunk<double>;

} // namespace kuzu::storage
