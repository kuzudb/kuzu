#include "storage/store/column_read.h"

#include <queue>

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

template<std::floating_point T>
class ExceptionChunk {
public:
    explicit ExceptionChunk(FloatColumnReader<T>* columnReader, Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage)
        : reader(columnReader), transaction(transaction),
          exceptionCount(metadata.compMeta.alpMetadata.exceptionCount),
          exceptionBaseCursor(getExceptionPageCursor(metadata,
              reader->getPageCursorForOffsetInGroup(0, metadata.pageIdx, numValuesPerPage),
              metadata.compMeta.alpMetadata.exceptionCapacity)) {}

    class ExceptionIter {
    public:
        explicit ExceptionIter(ExceptionChunk& chunk, size_t exceptionIdx)
            : chunk(chunk), exceptionIdx(exceptionIdx){};

        ExceptionIter operator+(size_t n) { return ExceptionIter{chunk, exceptionIdx + n}; }
        ExceptionIter& operator+=(size_t n) {
            exceptionIdx += n;
            return *this;
        }
        ExceptionIter operator-(const ExceptionIter& o) { return exceptionIdx - o.exceptionIdx; }
        EncodeException<T> operator*() { return chunk.getExceptionAt(exceptionIdx); }

    private:
        ExceptionChunk& chunk;
        size_t exceptionIdx;
    };

    void removeExceptionAt(size_t curExceptionIdx) {
        KU_ASSERT(curExceptionIdx < exceptionCount);
        for (size_t i = curExceptionIdx; i < exceptionCount - 1; ++i) {
            writeExceptionAt(getExceptionAt(i + 1), i);
        }
        --exceptionCount;
    }

    void addExceptions(std::span<const EncodeException<T>> exceptions) {
        // TODO could optimize
        for (size_t i = 0; i < exceptions.size(); ++i) {
            const offset_t desiredExceptionIdx =
                findFirstExceptionAtOrPastOffset(exceptions[i].posInPage);
            shiftExceptionsToRight(desiredExceptionIdx, 1);
            writeExceptionAt(exceptions[i], desiredExceptionIdx);
        }

        exceptionCount += exceptions.size();
    }

    EncodeException<T> getExceptionAt(size_t curExceptionIdx) {
        EncodeException<T> ret;

        PageCursor curExceptionCursor = PageUtils::getPageCursorForPos(curExceptionIdx,
            BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
        curExceptionCursor.pageIdx += exceptionBaseCursor.pageIdx;

        reader->readFromPage(transaction, curExceptionCursor.pageIdx,
            [&ret, &curExceptionCursor](uint8_t* frame) -> void {
                memcpy(&ret.value,
                    frame + EncodeException<T>::sizeBytes() * curExceptionCursor.elemPosInPage,
                    sizeof(ret.value));
                memcpy(&ret.posInPage,
                    frame + EncodeException<T>::sizeBytes() * curExceptionCursor.elemPosInPage +
                        sizeof(ret.value),
                    sizeof(ret.posInPage));
            });

        return ret;
    }

    void writeExceptionAt(EncodeException<T> exception, size_t curExceptionIdx) {
        PageCursor curExceptionCursor = PageUtils::getPageCursorForPos(curExceptionIdx,
            BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
        curExceptionCursor.pageIdx += exceptionBaseCursor.pageIdx;

        reader->updatePageWithCursor(curExceptionCursor, [&exception](auto* frame, auto posInPage) {
            std::memcpy(frame + posInPage * EncodeException<T>::sizeBytes(), &exception.value,
                sizeof(exception.value));
            std::memcpy(frame + posInPage * EncodeException<T>::sizeBytes() +
                            sizeof(exception.value),
                &exception.posInPage, sizeof(exception.posInPage));
        });
    }

    // TODO: can return a pair [idx, exception] so we don't need to duplicate read
    offset_t findFirstExceptionAtOrPastOffset(offset_t offsetInChunk) {
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

    static PageCursor getExceptionPageCursor(const ColumnChunkMetadata& metadata,
        PageCursor pageBaseCursor, size_t exceptionCapacity) {
        // TODO fix
        const size_t numExceptionPages = ceilDiv((uint64_t)exceptionCapacity,
            BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
        const size_t exceptionPageOffset = metadata.numPages - numExceptionPages;
        KU_ASSERT(exceptionPageOffset == (page_idx_t)exceptionPageOffset);
        return {pageBaseCursor.pageIdx + (page_idx_t)exceptionPageOffset, 0};
    }

    size_t& getExceptionCount() { return exceptionCount; }

private:
    void shiftExceptionsToRight(offset_t startExceptionIdx, size_t shift) {
        if (shift == 0) {
            return;
        }
        for (size_t i = exceptionCount - 1; i >= startExceptionIdx; --i) {
            writeExceptionAt(getExceptionAt(i), i + shift);
        }
    }

    FloatColumnReader<T>* reader;
    Transaction* transaction;
    size_t exceptionCount;
    PageCursor exceptionBaseCursor;
};

class DefaultColumnReader : public ColumnReader {
public:
    DefaultColumnReader(DBFileID dbFileID, BMFileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile)
        : ColumnReader(dbFileID, dataFH, bufferManager, shadowFile) {}

    void readCompressedValueToPage(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::offset_t nodeOffset,
        uint8_t* result, uint32_t offsetInResult,
        read_value_from_page_func_t<uint8_t*> readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, metadata, numValuesPerPage);
        readCompressedValue<uint8_t*>(transaction, metadata, cursor, offsetInChunk, result,
            offsetInResult, readFunc);
    }

    void readCompressedValueToVector(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::offset_t nodeOffset,
        common::ValueVector* result, uint32_t offsetInResult,
        read_value_from_page_func_t<common::ValueVector*> readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, metadata, numValuesPerPage);
        readCompressedValue<ValueVector*>(transaction, metadata, cursor, offsetInChunk, result,
            offsetInResult, readFunc);
    }

    uint64_t readCompressedValuesToPage(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, uint8_t* result,
        uint32_t startOffsetInResult, uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<uint8_t*> readFunc, filter_func_t filterFunc) override {
        return readCompressedValues(transaction, metadata, numValuesPerPage, result,
            startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::ValueVector* result,
        uint32_t startOffsetInResult, uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<common::ValueVector*> readFunc,
        filter_func_t filterFunc) override {
        return readCompressedValues(transaction, metadata, numValuesPerPage, result,
            startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    void writeValueToPageFromVector(ColumnChunkMetadata& chunkMetadata, uint64_t numValuesPerPage,
        common::offset_t offsetInChunk, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom,
        write_values_from_vector_func_t writeFromVectorFunc) override {
        writeValuesToPage(chunkMetadata, numValuesPerPage, offsetInChunk, vectorToWriteFrom,
            posInVectorToWriteFrom, 1, writeFromVectorFunc, &vectorToWriteFrom->getNullMask());
    }

    void writeValuesToPageFromBuffer(ColumnChunkMetadata& chunkMetadata, uint64_t numValuesPerPage,
        common::offset_t dstOffset, const uint8_t* data, const common::NullMask* nullChunkData,
        common::offset_t srcOffset, common::offset_t numValues,
        write_values_func_t writeFunc) override {
        writeValuesToPage(chunkMetadata, numValuesPerPage, dstOffset, data, srcOffset, numValues,
            writeFunc, nullChunkData);
    }

    template<typename InputType, typename... AdditionalArgs>
    void writeValuesToPage(ColumnChunkMetadata& chunkMetadata, uint64_t numValuesPerPage,
        common::offset_t dstOffset, InputType data, common::offset_t srcOffset,
        common::offset_t numValues,
        write_values_to_page_func_t<InputType, AdditionalArgs...> writeFunc,
        const NullMask* nullMask) {
        auto numValuesWritten = 0u;
        auto cursor =
            getPageCursorForOffsetInGroup(dstOffset, chunkMetadata.pageIdx, numValuesPerPage);
        while (numValuesWritten < numValues) {
            auto numValuesToWriteInPage =
                std::min(numValues - numValuesWritten, numValuesPerPage - cursor.elemPosInPage);
            updatePageWithCursor(cursor, [&](auto frame, auto offsetInPage) {
                if constexpr (std::is_same_v<InputType, ValueVector*>) {
                    writeFunc(frame, offsetInPage, data, srcOffset + numValuesWritten,
                        numValuesToWriteInPage, chunkMetadata.compMeta);
                } else {
                    writeFunc(frame, offsetInPage, data, srcOffset + numValuesWritten,
                        numValuesToWriteInPage, chunkMetadata.compMeta, nullMask);
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
    uint64_t readCompressedValues(Transaction* transaction, const ColumnChunkMetadata& metadata,
        uint64_t numValuesPerPage, OutputType result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<OutputType> readFunc, filter_func_t filterFunc) {
        const ColumnChunkMetadata& chunkMeta = metadata;
        const auto numValuesToScan = endNodeOffset - startNodeOffset;
        if (numValuesToScan == 0) {
            return 0;
        }

        auto pageCursor =
            getPageCursorForOffsetInGroup(startNodeOffset, metadata.pageIdx, numValuesPerPage);
        KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, metadata));

        uint64_t numValuesScanned = 0;
        while (numValuesScanned < numValuesToScan) {
            uint64_t numValuesToScanInPage = std::min(numValuesPerPage - pageCursor.elemPosInPage,
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

    void readCompressedValueToPage(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::offset_t nodeOffset,
        uint8_t* result, uint32_t offsetInResult,
        read_value_from_page_func_t<uint8_t*> readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, metadata, numValuesPerPage);
        readCompressedValue<uint8_t*>(transaction, metadata, numValuesPerPage, cursor,
            offsetInChunk, result, offsetInResult, readFunc);
    }

    void readCompressedValueToVector(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::offset_t nodeOffset,
        common::ValueVector* result, uint32_t offsetInResult,
        read_value_from_page_func_t<common::ValueVector*> readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, metadata, numValuesPerPage);
        readCompressedValue<ValueVector*>(transaction, metadata, numValuesPerPage, cursor,
            offsetInChunk, result, offsetInResult, readFunc);
    }

    uint64_t readCompressedValuesToPage(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, uint8_t* result,
        uint32_t startOffsetInResult, uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<uint8_t*> readFunc, filter_func_t filterFunc) override {
        return readCompressedValues(transaction, metadata, numValuesPerPage, result,
            startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::ValueVector* result,
        uint32_t startOffsetInResult, uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<common::ValueVector*> readFunc,
        filter_func_t filterFunc) override {
        return readCompressedValues(transaction, metadata, numValuesPerPage, result,
            startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    void writeValueToPageFromVector(ColumnChunkMetadata& metadata, uint64_t numValuesPerPage,
        common::offset_t offsetInChunk, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom,
        write_values_from_vector_func_t writeFromVectorFunc) override {
        if (metadata.compMeta.compression != CompressionType::FLOAT) {
            return defaultReader->writeValueToPageFromVector(metadata, numValuesPerPage,
                offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom, writeFromVectorFunc);
        }

        writeValuesToPage(metadata, numValuesPerPage, offsetInChunk, vectorToWriteFrom,
            posInVectorToWriteFrom, 1, writeFromVectorFunc, &vectorToWriteFrom->getNullMask());
    }

    void writeValuesToPageFromBuffer(ColumnChunkMetadata& chunkMetadata, uint64_t numValuesPerPage,
        common::offset_t dstOffset, const uint8_t* data, const common::NullMask* nullChunkData,
        common::offset_t srcOffset, common::offset_t numValues,
        write_values_func_t writeFunc) override {
        if (chunkMetadata.compMeta.compression != CompressionType::FLOAT) {
            return defaultReader->writeValuesToPageFromBuffer(chunkMetadata, numValuesPerPage,
                dstOffset, data, nullChunkData, srcOffset, numValues, writeFunc);
        }

        writeValuesToPage(chunkMetadata, numValuesPerPage, dstOffset, data, srcOffset, numValues,
            writeFunc, nullChunkData);
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
    void patchFloatExceptions(Transaction* transaction, const ColumnChunkMetadata& metadata,
        uint64_t numValuesPerPage, offset_t startOffsetInChunk, size_t numValuesToScan,
        OutputType result, offset_t startOffsetInResult, filter_func_t filterFunc) {
        // patch exceptions
        ExceptionChunk<T> exceptionChunk{this, transaction, metadata, numValuesPerPage};
        offset_t curExceptionIdx =
            exceptionChunk.findFirstExceptionAtOrPastOffset(startOffsetInChunk);
        for (; curExceptionIdx < exceptionChunk.getExceptionCount(); ++curExceptionIdx) {
            // TODO: potential optimization: read exceptions one page at a time
            const auto curException = exceptionChunk.getExceptionAt(curExceptionIdx);
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
    void readCompressedValue(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, PageCursor cursor,
        common::offset_t offsetInChunk, OutputType result, uint32_t offsetInResult,
        read_value_from_page_func_t<OutputType> readFunc) {
        KU_ASSERT(metadata.compMeta.compression == CompressionType::FLOAT ||
                  metadata.compMeta.compression == CompressionType::CONSTANT ||
                  metadata.compMeta.compression == CompressionType::UNCOMPRESSED);

        ExceptionChunk<T> exceptionChunk{this, transaction, metadata, numValuesPerPage};

        const offset_t firstExceptionIdx =
            exceptionChunk.findFirstExceptionAtOrPastOffset(offsetInChunk);

        do {
            if (metadata.compMeta.compression != CompressionType::FLOAT ||
                firstExceptionIdx == exceptionChunk.getExceptionCount()) {
                break;
            }

            const auto searchedException = exceptionChunk.getExceptionAt(firstExceptionIdx);
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
    uint64_t readCompressedValues(Transaction* transaction, const ColumnChunkMetadata& metadata,
        uint64_t numValuesPerPage, OutputType result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<OutputType> readFunc, filter_func_t filterFunc) {
        KU_ASSERT(metadata.compMeta.compression == CompressionType::FLOAT ||
                  metadata.compMeta.compression == CompressionType::CONSTANT ||
                  metadata.compMeta.compression == CompressionType::UNCOMPRESSED);

        const uint64_t numValuesToScan = endNodeOffset - startNodeOffset;
        const uint64_t numValuesScanned =
            defaultReader->readCompressedValues(transaction, metadata, numValuesPerPage, result,
                startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);

        if (metadata.compMeta.compression == CompressionType::FLOAT && numValuesScanned > 0) {
            patchFloatExceptions(transaction, metadata, numValuesPerPage, startNodeOffset,
                numValuesToScan, result, startOffsetInResult, filterFunc);
        }

        return numValuesScanned;
    }

    template<typename InputType, typename... AdditionalArgs>
    void writeValuesToPage(ColumnChunkMetadata& metadata, uint64_t numValuesPerPage,
        common::offset_t offsetInChunk, InputType data, uint32_t srcOffset, size_t numValues,
        write_values_to_page_func_t<InputType, AdditionalArgs...> writeFunc,
        const NullMask* nullMask) {

        // TODO: pass in from caller
        transaction::Transaction transaction{TransactionType::WRITE};
        ExceptionChunk<T> exceptionChunk{this, &transaction, metadata, numValuesPerPage};

        std::queue<offset_t> exceptionIndexesToRemove;
        std::queue<EncodeException<T>> exceptionsToAdd;

        size_t startExceptionIdxToSort = exceptionChunk.getExceptionCount();
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
                exceptionChunk.findFirstExceptionAtOrPastOffset(writeOffset);
            const auto curException = exceptionChunk.getExceptionAt(curExceptionIdx);

            if (curExceptionIdx < exceptionChunk.getExceptionCount() &&
                curException.posInPage == writeOffset) {
                exceptionIndexesToRemove.push(curExceptionIdx);
                startExceptionIdxToSort = std::min(startExceptionIdxToSort, curExceptionIdx);
            }

            if (newValue != decodedValue) {
                exceptionsToAdd.emplace(newValue, writeOffset);
            } else {
                defaultReader->writeValuesToPage(metadata, numValuesPerPage, writeOffset, data,
                    readOffset, 1, writeFunc, nullMask);
            }
        }

        // read exceptions to sort into buffer
        std::queue<EncodeException<T>> oldExceptions;
        for (offset_t i = startExceptionIdxToSort; i < exceptionChunk.getExceptionCount(); ++i) {
            if (!exceptionIndexesToRemove.empty() && exceptionIndexesToRemove.front() == i) {
                exceptionIndexesToRemove.pop();
            } else {
                oldExceptions.push(exceptionChunk.getExceptionAt(i));
            }
        }

        std::vector<EncodeException<T>> newExceptions;
        while (!oldExceptions.empty() || !exceptionsToAdd.empty()) {
            if (exceptionsToAdd.empty() ||
                oldExceptions.front().posInPage < exceptionsToAdd.front().posInPage) {
                newExceptions.push_back(oldExceptions.front());
                oldExceptions.pop();
            } else {
                newExceptions.push_back(exceptionsToAdd.front());
                exceptionsToAdd.pop();
            }
        }

        // write exceptions back into disk
        for (size_t i = 0; i < newExceptions.size(); ++i) {
            exceptionChunk.writeExceptionAt(newExceptions[i], startExceptionIdxToSort + i);
        }

        metadata.compMeta.alpMetadata.exceptionCount =
            startExceptionIdxToSort + newExceptions.size();
        KU_ASSERT(metadata.compMeta.alpMetadata.exceptionCount <=
                  metadata.compMeta.alpMetadata.exceptionCapacity);
        exceptionChunk.getExceptionCount() = metadata.compMeta.alpMetadata.exceptionCount;
    }

    std::unique_ptr<DefaultColumnReader> defaultReader;

    friend class ExceptionChunk<T>;
};

} // namespace

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
    common::offset_t nodeOffset, const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage) {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffsetInGroup(offsetInChunk, metadata.pageIdx, numValuesPerPage);
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, metadata));

    return {offsetInChunk, cursor};
}

} // namespace kuzu::storage
