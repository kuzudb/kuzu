#include "storage/store/compression_flush_buffer.h"

#include "storage/file_handle.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu::storage {
using namespace common;
using namespace transaction;

ColumnChunkMetadata uncompressedFlushBuffer(const uint8_t* buffer, uint64_t bufferSize,
    FileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    KU_ASSERT(dataFH->getNumPages() >= startPageIdx + metadata.numPages);
    if (dataFH->isInMemoryMode()) {
        const auto frame = dataFH->getFrame(startPageIdx);
        memcpy(frame, buffer, bufferSize);
    } else {
        dataFH->getFileInfo()->writeFile(buffer, bufferSize,
            startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    }
    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

ColumnChunkMetadata CompressedFlushBuffer::operator()(const uint8_t* buffer,
    uint64_t /*bufferSize*/, FileHandle* dataFH, common::page_idx_t startPageIdx,
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
        if (dataFH->isInMemoryMode()) {
            const auto frame = dataFH->getFrame(startPageIdx + numPages);
            memcpy(frame, compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE);
        } else {
            dataFH->getFileInfo()->writeFile(compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                (startPageIdx + numPages) * BufferPoolConstants::PAGE_4KB_SIZE);
        }
        numPages++;
    }
    // Make sure that the file is the right length
    if (numPages < metadata.numPages) {
        memset(compressedBuffer.get(), 0, BufferPoolConstants::PAGE_4KB_SIZE);
        if (dataFH->isInMemoryMode()) {
            const auto frame = dataFH->getFrame(startPageIdx + metadata.numPages - 1);
            memcpy(frame, compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE);
        } else {
            dataFH->getFileInfo()->writeFile(compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                (startPageIdx + metadata.numPages - 1) * BufferPoolConstants::PAGE_4KB_SIZE);
        }
    }
    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

namespace {
template<std::floating_point T>
std::pair<std::unique_ptr<uint8_t[]>, uint64_t> flushCompressedFloats(const CompressionAlg& alg,
    PhysicalTypeID dataType, const uint8_t* buffer, [[maybe_unused]] uint64_t bufferSize,
    FileHandle* dataFH, common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    const auto& castedAlg = ku_dynamic_cast<const CompressionAlg&, const FloatCompression<T>&>(alg);

    const auto* floatMetadata = metadata.compMeta.floatMetadata();
    KU_ASSERT(floatMetadata->exceptionCapacity >= floatMetadata->exceptionCount);

    auto valuesRemaining = metadata.numValues;
    KU_ASSERT(valuesRemaining <= bufferSize);

    const size_t exceptionBufferSize =
        EncodeException<T>::numPagesFromExceptions(floatMetadata->exceptionCapacity) *
        BufferPoolConstants::PAGE_4KB_SIZE;
    auto exceptionBuffer = std::make_unique<uint8_t[]>(exceptionBufferSize);
    std::byte* exceptionBufferCursor = reinterpret_cast<std::byte*>(exceptionBuffer.get());

    const auto numValuesPerPage =
        metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    KU_ASSERT(numValuesPerPage * metadata.numPages >= metadata.numValues);

    const auto compressedBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
    const uint8_t* bufferCursor = buffer;
    auto numPages = 0u;
    size_t remainingExceptionBufferSize = exceptionBufferSize;
    RUNTIME_CHECK(size_t totalExceptionCount = 0);

    while (valuesRemaining > 0) {
        uint64_t pageExceptionCount = 0;
        (void)castedAlg.compressNextPageWithExceptions(bufferCursor,
            metadata.numValues - valuesRemaining, valuesRemaining, compressedBuffer.get(),
            BufferPoolConstants::PAGE_4KB_SIZE, EncodeExceptionView<T>{exceptionBufferCursor},
            remainingExceptionBufferSize, pageExceptionCount, metadata.compMeta);

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
        if (dataFH->isInMemoryMode()) {
            const auto frame = dataFH->getFrame(startPageIdx + numPages);
            memcpy(frame, compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE);
        } else {
            dataFH->getFileInfo()->writeFile(compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                (startPageIdx + numPages) * BufferPoolConstants::PAGE_4KB_SIZE);
        }
        numPages++;
    }

    KU_ASSERT(totalExceptionCount == floatMetadata->exceptionCount);

    return {std::move(exceptionBuffer), exceptionBufferSize};
}

template<std::floating_point T>
void flushALPExceptions(const uint8_t* exceptionBuffer, uint64_t exceptionBufferSize,
    FileHandle* dataFH, common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    // we don't care about the min/max values for exceptions
    const auto preExceptionMetadata =
        uncompressedGetMetadata(exceptionBuffer, exceptionBufferSize, exceptionBufferSize,
            metadata.compMeta.floatMetadata()->exceptionCapacity, StorageValue{0}, StorageValue{0});

    const auto exceptionStartPageIdx =
        startPageIdx + metadata.numPages - preExceptionMetadata.numPages;
    KU_ASSERT(exceptionStartPageIdx + preExceptionMetadata.numPages <= dataFH->getNumPages());

    const auto encodedType = std::is_same_v<T, float> ? PhysicalTypeID::ALP_EXCEPTION_FLOAT :
                                                        PhysicalTypeID::ALP_EXCEPTION_DOUBLE;
    CompressedFlushBuffer exceptionFlushBuffer{
        std::make_shared<Uncompressed>(EncodeException<T>::sizeInBytes()), encodedType};
    (void)exceptionFlushBuffer.operator()(exceptionBuffer, exceptionBufferSize, dataFH,
        exceptionStartPageIdx, preExceptionMetadata);
}
} // namespace

template<std::floating_point T>
CompressedFloatFlushBuffer<T>::CompressedFloatFlushBuffer(std::shared_ptr<CompressionAlg> alg,
    common::PhysicalTypeID dataType)
    : alg{std::move(alg)}, dataType{dataType} {}

template<std::floating_point T>
CompressedFloatFlushBuffer<T>::CompressedFloatFlushBuffer(std::shared_ptr<CompressionAlg> alg,
    const common::LogicalType& dataType)
    : kuzu::storage::CompressedFloatFlushBuffer<T>(alg, dataType.getPhysicalType()) {}

template<std::floating_point T>
ColumnChunkMetadata CompressedFloatFlushBuffer<T>::operator()(const uint8_t* buffer,
    uint64_t bufferSize, FileHandle* dataFH, common::page_idx_t startPageIdx,
    const ColumnChunkMetadata& metadata) const {
    if (metadata.compMeta.compression == CompressionType::UNCOMPRESSED) {
        return CompressedFlushBuffer{std::make_shared<Uncompressed>(dataType), dataType}.operator()(
            buffer, bufferSize, dataFH, startPageIdx, metadata);
    }
    // FlushBuffer should not be called with constant compression
    KU_ASSERT(metadata.compMeta.compression == CompressionType::ALP);

    auto [exceptionBuffer, exceptionBufferSize] = flushCompressedFloats<T>(*alg, dataType, buffer,
        bufferSize, dataFH, startPageIdx, metadata);

    flushALPExceptions<T>(exceptionBuffer.get(), exceptionBufferSize, dataFH, startPageIdx,
        metadata);

    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

template class CompressedFloatFlushBuffer<float>;
template class CompressedFloatFlushBuffer<double>;

} // namespace kuzu::storage
