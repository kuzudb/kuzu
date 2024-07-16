#include "storage/store/column_read.h"

#include "common/utils.h"
#include "storage/compression/compression_float.h"
#include "storage/storage_structure/db_file_utils.h"
#include "storage/storage_utils.h"

namespace kuzu::storage {

using namespace common;
using namespace transaction;
namespace {
bool isPageIdxValid(page_idx_t pageIdx, const ColumnChunkMetadata& metadata) {
    return (metadata.pageIdx <= pageIdx && pageIdx < metadata.pageIdx + metadata.numPages) ||
           (pageIdx == INVALID_PAGE_IDX && metadata.compMeta.isConstant());
}

class DefaultColumnReader : public ColumnReader {
public:
    DefaultColumnReader(BMFileHandle* dataFH, BufferManager* bufferManager, WAL* wal)
        : ColumnReader(dataFH, bufferManager, wal) {}

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
        return readCompressedValuesFromDisk(transaction, metadata, numValuesPerPage, result,
            startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::ValueVector* result,
        uint32_t startOffsetInResult, uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<common::ValueVector*> readFunc,
        filter_func_t filterFunc) override {
        return readCompressedValuesFromDisk(transaction, metadata, numValuesPerPage, result,
            startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);
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
    uint64_t readCompressedValuesFromDisk(Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, OutputType result,
        uint32_t startOffsetInResult, uint64_t startNodeOffset, uint64_t endNodeOffset,
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
    FloatColumnReader(BMFileHandle* dataFH, BufferManager* bufferManager, WAL* wal)
        : ColumnReader(dataFH, bufferManager, wal),
          defaultReader(std::make_unique<DefaultColumnReader>(dataFH, bufferManager, wal)) {}

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

private:
    EncodeException<T> getExceptionAt(size_t curExceptionIdx, Transaction* transaction,
        PageCursor exceptionPageCursor) {
        EncodeException<T> ret;

        PageCursor curExceptionCursor = PageUtils::getPageCursorForPos(curExceptionIdx,
            BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
        curExceptionCursor.pageIdx += exceptionPageCursor.pageIdx;

        readFromPage(transaction, curExceptionCursor.pageIdx,
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

    offset_t findFirstExceptionAtOrPastOffset(Transaction* transaction, offset_t offsetInChunk,
        offset_t exceptionCount, PageCursor exceptionPageCursor) {
        // binary search for chunkOffset in exceptions

        offset_t lo = 0;
        offset_t hi = exceptionCount;
        while (lo < hi) {
            const size_t curExceptionIdx = (lo + hi) / 2;
            EncodeException<T> lastException =
                getExceptionAt(curExceptionIdx, transaction, exceptionPageCursor);

            if (lastException.posInPage < offsetInChunk) {
                lo = curExceptionIdx + 1;
            } else {
                hi = curExceptionIdx;
            }
        }

        return lo;
    }

    PageCursor getExceptionPageCursor(const ColumnChunkMetadata& metadata, PageCursor cursor) {
        // TODO fix
        const size_t numExceptionPages =
            ceilDiv((uint64_t)metadata.compMeta.alpMetadata.exceptionCount,
                BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
        const size_t exceptionPageOffset = metadata.numPages - numExceptionPages;
        KU_ASSERT(exceptionPageOffset == (page_idx_t)exceptionPageOffset);
        return {cursor.pageIdx + (page_idx_t)exceptionPageOffset, 0};
    }

    template<typename OutputType>
    void patchFloatExceptions(Transaction* transaction, const ColumnChunkMetadata& metadata,
        uint64_t numValuesPerPage, offset_t startOffsetInChunk, size_t numValuesToScan,
        OutputType result, offset_t startOffsetInResult, filter_func_t filterFunc) {
        // patch exceptions
        PageCursor exceptionPageCursor = getExceptionPageCursor(metadata,
            getPageCursorForOffsetInGroup(0, metadata.pageIdx, numValuesPerPage));
        offset_t curExceptionIdx = findFirstExceptionAtOrPastOffset(transaction, startOffsetInChunk,
            metadata.compMeta.alpMetadata.exceptionCount, exceptionPageCursor);
        for (; curExceptionIdx < metadata.compMeta.alpMetadata.exceptionCount; ++curExceptionIdx) {
            const auto curException =
                getExceptionAt(curExceptionIdx, transaction, exceptionPageCursor);
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

        PageCursor exceptionPageCursor = getExceptionPageCursor(metadata,
            getPageCursorForOffsetInGroup(0, metadata.pageIdx, numValuesPerPage));

        const offset_t firstExceptionIdx = findFirstExceptionAtOrPastOffset(transaction,
            offsetInChunk, metadata.compMeta.alpMetadata.exceptionCount, exceptionPageCursor);

        const auto searchedException =
            getExceptionAt(firstExceptionIdx, transaction, exceptionPageCursor);
        if (metadata.compMeta.compression != CompressionType::FLOAT ||
            firstExceptionIdx == metadata.compMeta.alpMetadata.exceptionCount ||
            searchedException.posInPage != offsetInChunk) {
            defaultReader->readCompressedValue(transaction, metadata, cursor, offsetInChunk, result,
                offsetInResult, readFunc);
        } else {
            KU_ASSERT(metadata.compMeta.compression == CompressionType::FLOAT);
            if constexpr (std::is_same_v<uint8_t*, OutputType>) {
                *reinterpret_cast<T*>(result) = searchedException.value;
            } else {
                *reinterpret_cast<T*>(result->getData()) = searchedException.value;
            }
        }
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
            defaultReader->readCompressedValuesFromDisk(transaction, metadata, numValuesPerPage,
                result, startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);

        if (metadata.compMeta.compression == CompressionType::FLOAT && numValuesScanned > 0) {
            patchFloatExceptions(transaction, metadata, numValuesPerPage, startNodeOffset,
                numValuesToScan, result, startOffsetInResult, filterFunc);
        }

        return numValuesScanned;
    }

    std::unique_ptr<DefaultColumnReader> defaultReader;
};

} // namespace

std::unique_ptr<ColumnReader> ColumnReaderFactory::createColumnReader(
    common::PhysicalTypeID dataType, BMFileHandle* dataFH, BufferManager* bufferManager, WAL* wal) {
    switch (dataType) {
    case common::PhysicalTypeID::FLOAT:
        return std::make_unique<FloatColumnReader<float>>(dataFH, bufferManager, wal);
    case common::PhysicalTypeID::DOUBLE:
        return std::make_unique<FloatColumnReader<double>>(dataFH, bufferManager, wal);
    default:
        return std::make_unique<DefaultColumnReader>(dataFH, bufferManager, wal);
    }
}

ColumnReader::ColumnReader(BMFileHandle* dataFH, BufferManager* bufferManager, WAL* wal)
    : dataFH(dataFH), bufferManager(bufferManager), wal(wal) {}

void ColumnReader::readFromPage(Transaction* transaction, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) {
    // For constant compression, call read on a nullptr since there is no data on disk and
    // decompression only requires metadata
    if (pageIdx == INVALID_PAGE_IDX) {
        return func(nullptr);
    }
    auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
        *dataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
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
