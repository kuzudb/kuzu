#include "storage/store/column_chunk_flush.h"

#include "common/utils.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu::storage {
using namespace common;
using namespace transaction;

ColumnChunkMetadata uncompressedFlushBuffer(const uint8_t* buffer, uint64_t bufferSize,
    BMFileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    KU_ASSERT(dataFH->getNumPages() >= startPageIdx + metadata.numPages);
    dataFH->getFileInfo()->writeFile(buffer, bufferSize,
        startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

ColumnChunkMetadata CompressedFlushBuffer::operator()(const uint8_t* buffer,
    uint64_t /*bufferSize*/, BMFileHandle* dataFH, common::page_idx_t startPageIdx,
    const ColumnChunkMetadata& metadata) const {
    auto valuesRemaining = metadata.numValues;
    const uint8_t* bufferStart = buffer;
    const auto compressedBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
    auto numPages = 0u;
    const auto numValuesPerPage =
        metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    KU_ASSERT(numValuesPerPage * metadata.numPages >= metadata.numValues);
    while (valuesRemaining > 0) {
        const auto compressedSize = alg->compressNextPage(bufferStart, valuesRemaining,
            compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE, metadata.compMeta);
        // Avoid underflows (when data is compressed to nothing, numValuesPerPage may be
        // UINT64_MAX)
        if (numValuesPerPage > valuesRemaining) {
            valuesRemaining = 0;
        } else {
            valuesRemaining -= numValuesPerPage;
        }
        if (compressedSize < BufferPoolConstants::PAGE_4KB_SIZE) {
            memset(compressedBuffer.get() + compressedSize, 0,
                BufferPoolConstants::PAGE_4KB_SIZE - compressedSize);
        }
        KU_ASSERT(numPages < metadata.numPages);
        KU_ASSERT(dataFH->getNumPages() > startPageIdx + numPages);
        dataFH->getFileInfo()->writeFile(compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE,
            (startPageIdx + numPages) * BufferPoolConstants::PAGE_4KB_SIZE);
        numPages++;
    }
    // Make sure that the file is the right length
    if (numPages < metadata.numPages) {
        memset(compressedBuffer.get(), 0, BufferPoolConstants::PAGE_4KB_SIZE);
        dataFH->getFileInfo()->writeFile(compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE,
            (startPageIdx + metadata.numPages - 1) * BufferPoolConstants::PAGE_4KB_SIZE);
    }
    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

template<std::floating_point T>
ColumnChunkMetadata CompressedFloatFlushBuffer<T>::operator()(const uint8_t* buffer,
    uint64_t bufferSize, BMFileHandle* dataFH, common::page_idx_t startPageIdx,
    const ColumnChunkMetadata& metadata) const {
    if (metadata.compMeta.compression == CompressionType::UNCOMPRESSED) {
        return CompressedFlushBuffer{std::make_shared<Uncompressed>(dataType), dataType}.operator()(
            buffer, bufferSize, dataFH, startPageIdx, metadata);
    }
    // FlushBuffer should not be called with constant compression
    KU_ASSERT(metadata.compMeta.compression == CompressionType::FLOAT);

    auto valuesRemaining = metadata.numValues;
    const uint8_t* bufferStart = buffer;
    const auto compressedBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
    const size_t exceptionBufferSize =
        ceilDiv(static_cast<uint64_t>(metadata.compMeta.alpMetadata.exceptionCapacity),
            (BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes())) *
        BufferPoolConstants::PAGE_4KB_SIZE;
    const auto exceptionBuffer = std::make_unique<uint8_t[]>(exceptionBufferSize);
    uint8_t* exceptionBufferCursor = exceptionBuffer.get();
    auto numPages = 0u;
    const auto numValuesPerPage =
        metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    KU_ASSERT(numValuesPerPage * metadata.numPages >= metadata.numValues);
    size_t remainingExceptionBufferSize = exceptionBufferSize;
    size_t totalExceptionCount = 0;
    while (valuesRemaining > 0) {
        uint64_t pageExceptionCount = 0;
        (void)alg->compressNextPageWithExceptions(bufferStart, metadata.numValues - valuesRemaining,
            valuesRemaining, compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE,
            exceptionBufferCursor, remainingExceptionBufferSize, pageExceptionCount,
            metadata.compMeta);
        exceptionBufferCursor += pageExceptionCount * EncodeException<T>::sizeBytes();
        KU_ASSERT(
            pageExceptionCount * EncodeException<T>::sizeBytes() <= remainingExceptionBufferSize);
        remainingExceptionBufferSize -= pageExceptionCount * EncodeException<T>::sizeBytes();
        totalExceptionCount += pageExceptionCount;

        // Avoid underflows (when data is compressed to nothing, numValuesPerPage may be
        // UINT64_MAX)
        if (numValuesPerPage > valuesRemaining) {
            valuesRemaining = 0;
        } else {
            valuesRemaining -= numValuesPerPage;
        }
        KU_ASSERT(numPages < metadata.numPages);
        KU_ASSERT(dataFH->getNumPages() > startPageIdx + numPages);
        dataFH->getFileInfo()->writeFile(compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE,
            (startPageIdx + numPages) * BufferPoolConstants::PAGE_4KB_SIZE);
        numPages++;
    }

    KU_ASSERT(exceptionBufferCursor <= exceptionBuffer.get() + exceptionBufferSize);
    KU_ASSERT(totalExceptionCount == metadata.compMeta.alpMetadata.exceptionCount);

    // set unused exception buffer entries to posInChunk = INVALID_POS
    for (size_t i = totalExceptionCount; i < metadata.compMeta.alpMetadata.exceptionCapacity; ++i) {
        EncodeException<T> invalidEntry{.value = 0, .posInChunk = EncodeException<T>::INVALID_POS};
        std::memcpy(exceptionBuffer.get() + i * EncodeException<T>::sizeBytes() +
                        sizeof(invalidEntry.value),
            &invalidEntry.posInChunk, sizeof(invalidEntry.posInChunk));
    }

    // check for underflow
    KU_ASSERT(remainingExceptionBufferSize <= exceptionBufferSize);

    const auto preExceptionMetadata =
        uncompressedGetMetadata(nullptr, exceptionBufferSize, exceptionBufferSize,
            metadata.compMeta.alpMetadata.exceptionCapacity, StorageValue{0}, StorageValue{1});

    const auto exceptionStartPageIdx =
        startPageIdx + metadata.numPages - preExceptionMetadata.numPages;
    KU_ASSERT(exceptionStartPageIdx + preExceptionMetadata.numPages <= dataFH->getNumPages());

    if constexpr (std::is_same_v<T, float>) {
        const auto encodedType = common::LogicalType::ALP_EXCEPTION_FLOAT();
        CompressedFlushBuffer exceptionFlushBuffer{std::make_shared<Uncompressed>(encodedType),
            encodedType};
        const auto exceptionMetadata =
            exceptionFlushBuffer.operator()(reinterpret_cast<const uint8_t*>(exceptionBuffer.get()),
                exceptionBufferSize, dataFH, exceptionStartPageIdx, preExceptionMetadata);

        return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
            metadata.compMeta);
    } else {
        const auto encodedType = common::LogicalType::ALP_EXCEPTION_DOUBLE();
        CompressedFlushBuffer exceptionFlushBuffer{std::make_shared<Uncompressed>(encodedType),
            encodedType};
        const auto exceptionMetadata =
            exceptionFlushBuffer.operator()(reinterpret_cast<const uint8_t*>(exceptionBuffer.get()),
                exceptionBufferSize, dataFH, exceptionStartPageIdx, preExceptionMetadata);

        return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
            metadata.compMeta);
    }
}

template class CompressedFloatFlushBuffer<float>;
template class CompressedFloatFlushBuffer<double>;

} // namespace kuzu::storage
