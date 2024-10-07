#include "storage/store/compression_flush_buffer.h"

#include "storage/file_handle.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu::storage {
using namespace common;
using namespace transaction;

ColumnChunkMetadata uncompressedFlushBuffer(std::span<const uint8_t> buffer, FileHandle* dataFH,
    page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    KU_ASSERT(dataFH->getNumPages() >= startPageIdx + metadata.numPages);
    dataFH->writePagesToFile(buffer.data(), buffer.size(), startPageIdx);
    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

ColumnChunkMetadata CompressedFlushBuffer::operator()(std::span<const uint8_t> buffer,
    FileHandle* dataFH, common::page_idx_t startPageIdx,
    const ColumnChunkMetadata& metadata) const {
    auto valuesRemaining = metadata.numValues;
    const uint8_t* bufferStart = buffer.data();
    const auto compressedBuffer = std::make_unique<uint8_t[]>(PAGE_SIZE);
    auto numPages = 0u;
    const auto numValuesPerPage = metadata.compMeta.numValues(PAGE_SIZE, dataType);
    KU_ASSERT(numValuesPerPage * metadata.numPages >= metadata.numValues);
    while (valuesRemaining > 0) {
        const auto compressedSize = alg->compressNextPage(bufferStart, valuesRemaining,
            compressedBuffer.get(), PAGE_SIZE, metadata.compMeta);
        // Avoid underflows (when data is compressed to nothing, numValuesPerPage may be
        // UINT64_MAX)
        if (numValuesPerPage > valuesRemaining) {
            valuesRemaining = 0;
        } else {
            valuesRemaining -= numValuesPerPage;
        }
        if (compressedSize < PAGE_SIZE) {
            memset(compressedBuffer.get() + compressedSize, 0, PAGE_SIZE - compressedSize);
        }
        KU_ASSERT(numPages < metadata.numPages);
        KU_ASSERT(dataFH->getNumPages() > startPageIdx + numPages);
        dataFH->writePageToFile(compressedBuffer.get(), startPageIdx + numPages);
        numPages++;
    }
    // Make sure that the on-disk file is the right length
    if (!dataFH->isInMemoryMode() && numPages < metadata.numPages) {
        memset(compressedBuffer.get(), 0, PAGE_SIZE);
        dataFH->writePageToFile(compressedBuffer.get(), startPageIdx + metadata.numPages - 1);
    }
    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

namespace {
template<std::floating_point T>
std::pair<std::unique_ptr<uint8_t[]>, uint64_t> flushCompressedFloats(const CompressionAlg& alg,
    PhysicalTypeID dataType, std::span<const uint8_t> buffer, FileHandle* dataFH,
    page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    const auto& castedAlg = ku_dynamic_cast<const FloatCompression<T>&>(alg);

    const auto* floatMetadata = metadata.compMeta.floatMetadata();
    KU_ASSERT(floatMetadata->exceptionCapacity >= floatMetadata->exceptionCount);

    auto valuesRemaining = metadata.numValues;
    KU_ASSERT(valuesRemaining <= buffer.size_bytes() / sizeof(T));

    const size_t exceptionBufferSize =
        EncodeException<T>::numPagesFromExceptions(floatMetadata->exceptionCapacity) * PAGE_SIZE;
    auto exceptionBuffer = std::make_unique<uint8_t[]>(exceptionBufferSize);
    std::byte* exceptionBufferCursor = reinterpret_cast<std::byte*>(exceptionBuffer.get());

    const auto numValuesPerPage = metadata.compMeta.numValues(PAGE_SIZE, dataType);
    KU_ASSERT(numValuesPerPage * metadata.numPages >= metadata.numValues);

    const auto compressedBuffer = std::make_unique<uint8_t[]>(PAGE_SIZE);
    const uint8_t* bufferCursor = buffer.data();
    auto numPages = 0u;
    size_t remainingExceptionBufferSize = exceptionBufferSize;
    RUNTIME_CHECK(size_t totalExceptionCount = 0);

    while (valuesRemaining > 0) {
        uint64_t pageExceptionCount = 0;
        (void)castedAlg.compressNextPageWithExceptions(bufferCursor,
            metadata.numValues - valuesRemaining, valuesRemaining, compressedBuffer.get(),
            PAGE_SIZE, EncodeExceptionView<T>{exceptionBufferCursor}, remainingExceptionBufferSize,
            pageExceptionCount, metadata.compMeta);

        exceptionBufferCursor += pageExceptionCount * EncodeException<T>::sizeInBytes();
        remainingExceptionBufferSize -= pageExceptionCount * EncodeException<T>::sizeInBytes();
        RUNTIME_CHECK(totalExceptionCount += pageExceptionCount);

        // Avoid underflows (when data is compressed to nothing, numValuesPerPage may be
        // UINT64_MAX)
        if (numValuesPerPage > valuesRemaining) {
            valuesRemaining = 0;
        } else {
            valuesRemaining -= numValuesPerPage;
        }
        KU_ASSERT(numPages < metadata.numPages);
        KU_ASSERT(dataFH->getNumPages() > startPageIdx + numPages);
        dataFH->writePageToFile(compressedBuffer.get(), startPageIdx + numPages);
        numPages++;
    }

    KU_ASSERT(totalExceptionCount == floatMetadata->exceptionCount);

    return {std::move(exceptionBuffer), exceptionBufferSize};
}

template<std::floating_point T>
void flushALPExceptions(std::span<const uint8_t> exceptionBuffer, FileHandle* dataFH,
    page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    // we don't care about the min/max values for exceptions
    const auto preExceptionMetadata =
        uncompressedGetMetadata(exceptionBuffer, exceptionBuffer.size(),
            metadata.compMeta.floatMetadata()->exceptionCapacity, StorageValue{0}, StorageValue{0});

    const auto exceptionStartPageIdx =
        startPageIdx + metadata.numPages - preExceptionMetadata.numPages;
    KU_ASSERT(exceptionStartPageIdx + preExceptionMetadata.numPages <= dataFH->getNumPages());

    const auto encodedType = std::is_same_v<T, float> ? PhysicalTypeID::ALP_EXCEPTION_FLOAT :
                                                        PhysicalTypeID::ALP_EXCEPTION_DOUBLE;
    CompressedFlushBuffer exceptionFlushBuffer{
        std::make_shared<Uncompressed>(EncodeException<T>::sizeInBytes()), encodedType};
    (void)exceptionFlushBuffer.operator()(exceptionBuffer, dataFH, exceptionStartPageIdx,
        preExceptionMetadata);
}
} // namespace

template<std::floating_point T>
CompressedFloatFlushBuffer<T>::CompressedFloatFlushBuffer(std::shared_ptr<CompressionAlg> alg,
    PhysicalTypeID dataType)
    : alg{std::move(alg)}, dataType{dataType} {}

template<std::floating_point T>
CompressedFloatFlushBuffer<T>::CompressedFloatFlushBuffer(std::shared_ptr<CompressionAlg> alg,
    const LogicalType& dataType)
    : CompressedFloatFlushBuffer<T>(alg, dataType.getPhysicalType()) {}

template<std::floating_point T>
ColumnChunkMetadata CompressedFloatFlushBuffer<T>::operator()(std::span<const uint8_t> buffer,
    FileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) const {
    if (metadata.compMeta.compression == CompressionType::UNCOMPRESSED) {
        return CompressedFlushBuffer{std::make_shared<Uncompressed>(dataType), dataType}.operator()(
            buffer, dataFH, startPageIdx, metadata);
    }
    // FlushBuffer should not be called with constant compression
    KU_ASSERT(metadata.compMeta.compression == CompressionType::ALP);

    auto [exceptionBuffer, exceptionBufferSize] =
        flushCompressedFloats<T>(*alg, dataType, buffer, dataFH, startPageIdx, metadata);

    flushALPExceptions<T>(std::span<const uint8_t>(exceptionBuffer.get(), exceptionBufferSize),
        dataFH, startPageIdx, metadata);

    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

template class CompressedFloatFlushBuffer<float>;
template class CompressedFloatFlushBuffer<double>;

} // namespace kuzu::storage
