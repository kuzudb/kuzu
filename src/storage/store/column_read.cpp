#include "storage/store/column_read.h"

#include "alp/encode.hpp"
#include "common/utils.h"
#include "storage/compression/compression_float.h"
#include "storage/storage_structure/db_file_utils.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"

namespace kuzu::storage {

using namespace common;
using namespace transaction;
namespace {
bool isPageIdxValid(page_idx_t pageIdx, const ColumnChunkMetadata& metadata) {
    return (metadata.pageIdx <= pageIdx && pageIdx < metadata.pageIdx + metadata.numPages) ||
           (pageIdx == INVALID_PAGE_IDX && metadata.compMeta.isConstant());
}

template<std::floating_point T>
class FloatColumnReader;

class DefaultColumnReader : public ColumnReader {
public:
    DefaultColumnReader(DBFileID dbFileID, BMFileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile)
        : ColumnReader(dbFileID, dataFH, bufferManager, shadowFile) {}

    void readCompressedValueToPage(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, uint8_t* result, uint32_t offsetInResult,
        read_value_from_page_func_t<uint8_t*> readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<uint8_t*>(transaction, state.metadata, cursor, offsetInChunk, result,
            offsetInResult, readFunc);
    }

    void readCompressedValueToVector(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* result, uint32_t offsetInResult,
        read_value_from_page_func_t<common::ValueVector*> readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<ValueVector*>(transaction, state.metadata, cursor, offsetInChunk,
            result, offsetInResult, readFunc);
    }

    uint64_t readCompressedValuesToPage(transaction::Transaction* transaction,
        const ChunkState& state, uint8_t* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<uint8_t*> readFunc, filter_func_t filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ChunkState& state, common::ValueVector* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<common::ValueVector*> readFunc,
        filter_func_t filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    void writeValueToPageFromVector(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom,
        write_values_from_vector_func_t writeFromVectorFunc) override {
        writeValuesToPage(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom, 1,
            writeFromVectorFunc, &vectorToWriteFrom->getNullMask());
    }

    void writeValuesToPageFromBuffer(ChunkState& state, common::offset_t dstOffset,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset,
        common::offset_t numValues, write_values_func_t writeFunc) override {
        writeValuesToPage(state, dstOffset, data, srcOffset, numValues, writeFunc, nullChunkData);
    }

    template<typename InputType, typename... AdditionalArgs>
    void writeValuesToPage(ChunkState& state, common::offset_t dstOffset, InputType data,
        common::offset_t srcOffset, common::offset_t numValues,
        write_values_to_page_func_t<InputType, AdditionalArgs...> writeFunc,
        const NullMask* nullMask) {
        auto numValuesWritten = 0u;
        auto cursor = getPageCursorForOffsetInGroup(dstOffset, state.metadata.pageIdx,
            state.numValuesPerPage);
        while (numValuesWritten < numValues) {
            auto numValuesToWriteInPage = std::min(numValues - numValuesWritten,
                state.numValuesPerPage - cursor.elemPosInPage);
            updatePageWithCursor(cursor, [&](auto frame, auto offsetInPage) {
                if constexpr (std::is_same_v<InputType, ValueVector*>) {
                    writeFunc(frame, offsetInPage, data, srcOffset + numValuesWritten,
                        numValuesToWriteInPage, state.metadata.compMeta);
                } else {
                    writeFunc(frame, offsetInPage, data, srcOffset + numValuesWritten,
                        numValuesToWriteInPage, state.metadata.compMeta, nullMask);
                }
            });
            numValuesWritten += numValuesToWriteInPage;
            cursor.nextPage();
        }
    }

    template<typename OutputType>
    void readCompressedValue(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, PageCursor cursor, common::offset_t /*offsetInChunk*/,
        OutputType result, uint32_t offsetInResult,
        read_value_from_page_func_t<OutputType> readFunc) {

        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            readFunc(frame, cursor, result, offsetInResult, 1 /* numValuesToRead */,
                metadata.compMeta);
        });
    }

    template<typename OutputType>
    uint64_t readCompressedValues(Transaction* transaction, const ChunkState& state,
        OutputType result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, read_values_from_page_func_t<OutputType> readFunc,
        filter_func_t filterFunc) {
        const ColumnChunkMetadata& chunkMeta = state.metadata;
        const auto numValuesToScan = endNodeOffset - startNodeOffset;
        if (numValuesToScan == 0) {
            return 0;
        }

        auto pageCursor = getPageCursorForOffsetInGroup(startNodeOffset, chunkMeta.pageIdx,
            state.numValuesPerPage);
        KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));

        uint64_t numValuesScanned = 0;
        while (numValuesScanned < numValuesToScan) {
            uint64_t numValuesToScanInPage =
                std::min(state.numValuesPerPage - pageCursor.elemPosInPage,
                    numValuesToScan - numValuesScanned);
            KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
            if (filterFunc(numValuesScanned, numValuesScanned + numValuesToScanInPage)) {
                readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                    readFunc(frame, pageCursor, result, numValuesScanned + startOffsetInResult,
                        numValuesToScanInPage, chunkMeta.compMeta);
                });
            }
            numValuesScanned += numValuesToScanInPage;
            pageCursor.nextPage();
        }

        return numValuesScanned;
    }
};

template<std::floating_point T>
class FloatColumnReader : public ColumnReader {
public:
    FloatColumnReader(DBFileID dbFileID, BMFileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile)
        : ColumnReader(dbFileID, dataFH, bufferManager, shadowFile),
          defaultReader(
              std::make_unique<DefaultColumnReader>(dbFileID, dataFH, bufferManager, shadowFile)) {}

    void readCompressedValueToPage(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, uint8_t* result, uint32_t offsetInResult,
        read_value_from_page_func_t<uint8_t*> readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<uint8_t*>(transaction, state, cursor, offsetInChunk, result,
            offsetInResult, readFunc);
    }

    void readCompressedValueToVector(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* result, uint32_t offsetInResult,
        read_value_from_page_func_t<common::ValueVector*> readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<ValueVector*>(transaction, state, cursor, offsetInChunk, result,
            offsetInResult, readFunc);
    }

    uint64_t readCompressedValuesToPage(transaction::Transaction* transaction,
        const ChunkState& state, uint8_t* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<uint8_t*> readFunc, filter_func_t filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ChunkState& state, common::ValueVector* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<common::ValueVector*> readFunc,
        filter_func_t filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    void writeValueToPageFromVector(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom,
        write_values_from_vector_func_t writeFromVectorFunc) override {
        if (state.metadata.compMeta.compression != CompressionType::FLOAT) {
            return defaultReader->writeValueToPageFromVector(state, offsetInChunk,
                vectorToWriteFrom, posInVectorToWriteFrom, writeFromVectorFunc);
        }

        writeValuesToPage(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom, 1,
            writeFromVectorFunc, &vectorToWriteFrom->getNullMask());
    }

    void writeValuesToPageFromBuffer(ChunkState& state, common::offset_t dstOffset,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset,
        common::offset_t numValues, write_values_func_t writeFunc) override {
        if (state.metadata.compMeta.compression != CompressionType::FLOAT) {
            return defaultReader->writeValuesToPageFromBuffer(state, dstOffset, data, nullChunkData,
                srcOffset, numValues, writeFunc);
        }

        writeValuesToPage(state, dstOffset, data, srcOffset, numValues, writeFunc, nullChunkData);
    }

private:
    template<typename InputType>
    T getValueFromInput(InputType data, uint32_t srcOffset) {
        if constexpr (std::is_same_v<decltype(data), const uint8_t*>) {
            return reinterpret_cast<const T*>(data)[srcOffset];
        } else {
            static_assert(std::is_same_v<std::remove_const_t<decltype(data)>, ValueVector*>);
            return data->template getValue<T>(srcOffset);
        }
    }

    template<typename OutputType>
    void patchFloatExceptions(const ChunkState& state, offset_t startOffsetInChunk,
        size_t numValuesToScan, OutputType result, offset_t startOffsetInResult,
        filter_func_t filterFunc) {
        // patch exceptions
        auto* exceptionChunk = state.getExceptionChunkConst<T>();
        offset_t curExceptionIdx =
            exceptionChunk->findFirstExceptionAtOrPastOffset(startOffsetInChunk);
        for (; curExceptionIdx < exceptionChunk->getExceptionCount(); ++curExceptionIdx) {
            // TODO: potential optimization: read exceptions one page at a time
            const auto curException = exceptionChunk->getExceptionAt(curExceptionIdx);
            KU_ASSERT(curException.posInPage >= curExceptionIdx);
            if (curException.posInPage >= startOffsetInChunk + numValuesToScan) {
                break;
            }
            const offset_t offsetInResult =
                startOffsetInResult + curException.posInPage - startOffsetInChunk;
            if (filterFunc(offsetInResult, offsetInResult + 1)) {
                if constexpr (std::is_same_v<uint8_t*, OutputType>) {
                    reinterpret_cast<T*>(result)[offsetInResult] = curException.value;
                } else {
                    static_assert(std::is_same_v<ValueVector*, std::remove_cvref_t<OutputType>>);
                    reinterpret_cast<T*>(result->getData())[offsetInResult] = curException.value;
                }
            }
        }
    }

    template<typename OutputType>
    void readCompressedValue(transaction::Transaction* transaction, const ChunkState& state,
        PageCursor cursor, common::offset_t offsetInChunk, OutputType result,
        uint32_t offsetInResult, read_value_from_page_func_t<OutputType> readFunc) {
        const ColumnChunkMetadata& metadata = state.metadata;
        KU_ASSERT(metadata.compMeta.compression == CompressionType::FLOAT ||
                  metadata.compMeta.compression == CompressionType::CONSTANT ||
                  metadata.compMeta.compression == CompressionType::UNCOMPRESSED);

        auto* exceptionChunk = state.getExceptionChunkConst<T>();

        const offset_t firstExceptionIdx =
            exceptionChunk->findFirstExceptionAtOrPastOffset(offsetInChunk);

        do {
            if (metadata.compMeta.compression != CompressionType::FLOAT ||
                firstExceptionIdx == exceptionChunk->getExceptionCount()) {
                break;
            }

            const auto searchedException = exceptionChunk->getExceptionAt(firstExceptionIdx);
            if (searchedException.posInPage != offsetInChunk) {
                break;
            }

            KU_ASSERT(metadata.compMeta.compression == CompressionType::FLOAT);
            if constexpr (std::is_same_v<uint8_t*, OutputType>) {
                *reinterpret_cast<T*>(result) = searchedException.value;
            } else {
                *reinterpret_cast<T*>(result->getData()) = searchedException.value;
            }
            return;
        } while (false);

        defaultReader->readCompressedValue(transaction, metadata, cursor, offsetInChunk, result,
            offsetInResult, readFunc);
    }

    template<typename OutputType>
    uint64_t readCompressedValues(Transaction* transaction, const ChunkState& state,
        OutputType result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, read_values_from_page_func_t<OutputType> readFunc,
        filter_func_t filterFunc) {
        const ColumnChunkMetadata& metadata = state.metadata;
        KU_ASSERT(metadata.compMeta.compression == CompressionType::FLOAT ||
                  metadata.compMeta.compression == CompressionType::CONSTANT ||
                  metadata.compMeta.compression == CompressionType::UNCOMPRESSED);

        const uint64_t numValuesToScan = endNodeOffset - startNodeOffset;
        const uint64_t numValuesScanned = defaultReader->readCompressedValues(transaction, state,
            result, startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);

        if (metadata.compMeta.compression == CompressionType::FLOAT && numValuesScanned > 0) {
            patchFloatExceptions(state, startNodeOffset, numValuesToScan, result,
                startOffsetInResult, filterFunc);
        }

        return numValuesScanned;
    }

    template<typename InputType, typename... AdditionalArgs>
    void writeValuesToPage(ChunkState& state, common::offset_t offsetInChunk, InputType data,
        uint32_t srcOffset, size_t numValues,
        write_values_to_page_func_t<InputType, AdditionalArgs...> writeFunc,
        const NullMask* nullMask) {
        const ColumnChunkMetadata& metadata = state.metadata;

        auto* exceptionChunk = state.getExceptionChunk<T>();

        size_t startExceptionIdxToSort = exceptionChunk->getExceptionCount();
        for (size_t i = 0; i < numValues; ++i) {

            const size_t writeOffset = offsetInChunk + i;
            const size_t readOffset = srcOffset + i;

            if (nullMask && nullMask->isNull(readOffset)) {
                continue;
            }

            const T newValue = getValueFromInput(data, readOffset);
            const int64_t encodedValue = alp::AlpEncode<T>::encode_value(newValue,
                metadata.compMeta.alpMetadata.fac, metadata.compMeta.alpMetadata.exp);
            const T decodedValue = alp::AlpDecode<T>::decode_value(encodedValue,
                metadata.compMeta.alpMetadata.fac, metadata.compMeta.alpMetadata.exp);

            const offset_t curExceptionIdx =
                exceptionChunk->findFirstExceptionAtOrPastOffset(writeOffset);

            if (curExceptionIdx < exceptionChunk->getExceptionCount()) {
                const auto curException = exceptionChunk->getExceptionAt(curExceptionIdx);
                if (curException.posInPage == writeOffset) {
                    exceptionChunk->removeExceptionAt(curExceptionIdx);
                    startExceptionIdxToSort = std::min(startExceptionIdxToSort, curExceptionIdx);
                }
            }

            if (newValue != decodedValue) {
                KU_ASSERT(static_cast<uint32_t>(writeOffset) == writeOffset);
                exceptionChunk->addException(
                    EncodeException<T>{newValue, static_cast<uint32_t>(writeOffset)});
            } else {
                defaultReader->writeValuesToPage(state, writeOffset, data, readOffset, 1, writeFunc,
                    nullMask);
            }
        }
    }

    std::unique_ptr<DefaultColumnReader> defaultReader;
};

template<std::floating_point T>
struct ExceptionBufferElementView {
    using type = EncodeException<T>;

    explicit ExceptionBufferElementView(std::byte* val) { bytes = val; }
    explicit ExceptionBufferElementView(T* val) { bytes = reinterpret_cast<std::byte*>(val); }
    explicit ExceptionBufferElementView(T& val) { bytes = reinterpret_cast<std::byte*>(&val); }

    EncodeException<T> getValue() const {
        EncodeException<T> ret;
        std::memcpy(&ret.value, bytes, sizeof(ret.value));
        std::memcpy(&ret.posInPage, bytes + sizeof(ret.value), sizeof(ret.posInPage));
        return ret;
    }
    void setValue(EncodeException<T> exception) {
        std::memcpy(bytes, &exception.value, sizeof(exception.value));
        std::memcpy(bytes + sizeof(exception.value), &exception.posInPage,
            sizeof(exception.posInPage));
    }
    bool operator<(const ExceptionBufferElementView<T>& o) {
        return getValue().posInPage < o.getValue().posInPage;
    }
    std::byte* bytes;
};

} // namespace

template<std::floating_point T>
InMemoryExceptionChunk<T>::InMemoryExceptionChunk(ColumnReader* columnReader,
    Transaction* transaction, const ChunkState& state)
    : exceptionCount(state.metadata.compMeta.alpMetadata.exceptionCount),
      exceptionCapacity(state.metadata.compMeta.alpMetadata.exceptionCapacity),
      exceptionBuffer(
          std::make_unique<std::byte[]>(exceptionCapacity * EncodeException<T>::sizeBytes())),
      emptyMask(exceptionCapacity) {
    emptyMask.setNullFromRange(exceptionCount, exceptionCapacity - exceptionCount, true);

    auto exceptionCursor = getExceptionPageCursor(state.metadata,
        columnReader->getPageCursorForOffsetInGroup(0, state.metadata.pageIdx,
            state.numValuesPerPage),
        exceptionCapacity);
    KU_ASSERT(exceptionCursor.elemPosInPage == 0);

    const size_t numExceptionPages = ceilDiv((uint64_t)exceptionCapacity,
        BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
    size_t remainingBytesToCopy = exceptionCapacity * EncodeException<T>::sizeBytes();
    for (size_t i = 0; i < numExceptionPages; ++i) {
        static constexpr size_t exceptionBytesPerPage = BufferPoolConstants::PAGE_4KB_SIZE /
                                                        EncodeException<T>::sizeBytes() *
                                                        EncodeException<T>::sizeBytes();
        columnReader->readFromPage(transaction, exceptionCursor.pageIdx,
            [this, i, remainingBytesToCopy](uint8_t* frame) {
                std::memcpy(exceptionBuffer.get() + i * exceptionBytesPerPage, frame,
                    std::min(exceptionBytesPerPage, remainingBytesToCopy));
            });

        remainingBytesToCopy -= exceptionBytesPerPage;
        KU_ASSERT(remainingBytesToCopy >= 0);
        exceptionCursor.nextPage();
    }
}

template<std::floating_point T>
void InMemoryExceptionChunk<T>::flushToDisk(ColumnReader* columnReader, ChunkState& state) {
    // removes holes + sorts exception chunk
    size_t actualExceptionCount = 0;
    for (size_t i = 0; i < exceptionCount; ++i) {
        if (!emptyMask.isNull(i)) {
            ++actualExceptionCount;
            if (actualExceptionCount - 1 == i) {
                continue;
            }
            writeException(getExceptionAt(i), actualExceptionCount - 1);
        }
    }

    KU_ASSERT(actualExceptionCount <= state.metadata.compMeta.alpMetadata.exceptionCapacity);
    state.metadata.compMeta.alpMetadata.exceptionCount = actualExceptionCount;

    using exceptionWord = std::array<std::byte, EncodeException<T>::sizeBytes()>;
    exceptionWord* exceptionWordBuffer = reinterpret_cast<exceptionWord*>(exceptionBuffer.get());
    std::sort(exceptionWordBuffer, exceptionWordBuffer + actualExceptionCount,
        [](exceptionWord& a, exceptionWord& b) {
            return ExceptionBufferElementView<T>{reinterpret_cast<std::byte*>(&a)} <
                   ExceptionBufferElementView<T>{reinterpret_cast<std::byte*>(&b)};
        });
    std::memset(exceptionBuffer.get() + actualExceptionCount * EncodeException<T>::sizeBytes(), 0,
        (exceptionCount - actualExceptionCount) * EncodeException<T>::sizeBytes());
    emptyMask.setNullFromRange(actualExceptionCount, (exceptionCount - actualExceptionCount), true);
    exceptionCount = actualExceptionCount;

    auto exceptionCursor = getExceptionPageCursor(state.metadata,
        columnReader->getPageCursorForOffsetInGroup(0, state.metadata.pageIdx,
            state.numValuesPerPage),
        exceptionCapacity);

    const size_t numExceptionPages = ceilDiv((uint64_t)exceptionCapacity,
        BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
    size_t remainingBytesToCopy = exceptionCapacity * EncodeException<T>::sizeBytes();
    for (size_t i = 0; i < numExceptionPages; ++i) {
        static constexpr size_t exceptionBytesPerPage = BufferPoolConstants::PAGE_4KB_SIZE /
                                                        EncodeException<T>::sizeBytes() *
                                                        EncodeException<T>::sizeBytes();
        KU_ASSERT(exceptionCursor.elemPosInPage == 0);
        columnReader->updatePageWithCursor(exceptionCursor,
            [this, i, remainingBytesToCopy](auto frame, auto /*offsetInPage*/) {
                std::memcpy(frame, exceptionBuffer.get() + i * exceptionBytesPerPage,
                    std::min(exceptionBytesPerPage, remainingBytesToCopy));
                // TODO: zero remaining bytes
            });

        remainingBytesToCopy -= exceptionBytesPerPage;
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
    EncodeException<T> ret;
    auto* exceptionBufferEntry =
        exceptionBuffer.get() + exceptionIdx * EncodeException<T>::sizeBytes();
    std::memcpy(&ret.value, exceptionBufferEntry, sizeof(ret.value));
    std::memcpy(&ret.posInPage, exceptionBufferEntry + sizeof(ret.value), sizeof(ret.posInPage));
    return ret;
}

template<std::floating_point T>
void InMemoryExceptionChunk<T>::writeException(EncodeException<T> exception, size_t exceptionIdx) {
    KU_ASSERT(exceptionIdx < exceptionCount);
    // use memcpy instead of direct assign as we don't want to copy struct padding
    auto* exceptionBufferEntry =
        exceptionBuffer.get() + exceptionIdx * EncodeException<T>::sizeBytes();
    std::memcpy(exceptionBufferEntry, &exception.value, sizeof(exception.value));
    std::memcpy(exceptionBufferEntry + sizeof(exception.value), &exception.posInPage,
        sizeof(exception.posInPage));
}

// TODO: can return a pair [idx, exception] so we don't need to duplicate read
template<std::floating_point T>
offset_t InMemoryExceptionChunk<T>::findFirstExceptionAtOrPastOffset(offset_t offsetInChunk) const {
    // binary search for chunkOffset in exceptions

    offset_t lo = 0;
    offset_t hi = exceptionCount;
    while (lo < hi) {
        const size_t curExceptionIdx = (lo + hi) / 2;
        EncodeException<T> lastException = getExceptionAt(curExceptionIdx);

        if (lastException.posInPage < offsetInChunk) {
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
    // TODO fix
    const size_t numExceptionPages = ceilDiv((uint64_t)exceptionCapacity,
        BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
    const size_t exceptionPageOffset = metadata.numPages - numExceptionPages;
    KU_ASSERT(exceptionPageOffset == (page_idx_t)exceptionPageOffset);
    return {pageBaseCursor.pageIdx + (page_idx_t)exceptionPageOffset, 0};
}

template<std::floating_point T>
size_t InMemoryExceptionChunk<T>::getExceptionCount() const {
    return exceptionCount;
}

template class InMemoryExceptionChunk<float>;
template class InMemoryExceptionChunk<double>;

std::unique_ptr<ColumnReader> ColumnReaderFactory::createColumnReader(
    common::PhysicalTypeID dataType, DBFileID dbFileID, BMFileHandle* dataFH,
    BufferManager* bufferManager, ShadowFile* shadowFile) {
    switch (dataType) {
    case common::PhysicalTypeID::FLOAT:
        return std::make_unique<FloatColumnReader<float>>(dbFileID, dataFH, bufferManager,
            shadowFile);
    case common::PhysicalTypeID::DOUBLE:
        return std::make_unique<FloatColumnReader<double>>(dbFileID, dataFH, bufferManager,
            shadowFile);
    default:
        return std::make_unique<DefaultColumnReader>(dbFileID, dataFH, bufferManager, shadowFile);
    }
}

ColumnReader::ColumnReader(DBFileID dbFileID, BMFileHandle* dataFH, BufferManager* bufferManager,
    ShadowFile* shadowFile)
    : dbFileID(dbFileID), dataFH(dataFH), bufferManager(bufferManager), shadowFile(shadowFile) {}

void ColumnReader::readFromPage(Transaction* transaction, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) {
    // For constant compression, call read on a nullptr since there is no data on disk and
    // decompression only requires metadata
    if (pageIdx == INVALID_PAGE_IDX) {
        return func(nullptr);
    }
    auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
        *dataFH, pageIdx, *shadowFile, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

void ColumnReader::updatePageWithCursor(PageCursor cursor,
    const std::function<void(uint8_t*, common::offset_t)>& writeOp) const {
    bool insertingNewPage = false;
    if (cursor.pageIdx == INVALID_PAGE_IDX) {
        return writeOp(nullptr, cursor.elemPosInPage);
    }
    if (cursor.pageIdx >= dataFH->getNumPages()) {
        KU_ASSERT(cursor.pageIdx == dataFH->getNumPages());
        DBFileUtils::insertNewPage(*dataFH, dbFileID, *bufferManager, *shadowFile);
        insertingNewPage = true;
    }
    DBFileUtils::updatePage(*dataFH, dbFileID, cursor.pageIdx, insertingNewPage, *bufferManager,
        *shadowFile, [&](auto frame) { writeOp(frame, cursor.elemPosInPage); });
}

PageCursor ColumnReader::getPageCursorForOffsetInGroup(offset_t offsetInChunk,
    page_idx_t groupPageIdx, uint64_t numValuesPerPage) const {
    auto pageCursor = PageUtils::getPageCursorForPos(offsetInChunk, numValuesPerPage);
    pageCursor.pageIdx += groupPageIdx;
    return pageCursor;
}

std::pair<common::offset_t, PageCursor> ColumnReader::getOffsetAndCursor(
    common::offset_t nodeOffset, const ChunkState& state) {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffsetInGroup(offsetInChunk, state.metadata.pageIdx,
        state.numValuesPerPage);
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, state.metadata));

    return {offsetInChunk, cursor};
}

} // namespace kuzu::storage
