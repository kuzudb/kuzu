#pragma once

#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/compression/compression.h"
#include "storage/compression/float_compression.h"
#include "storage/store/column_chunk_metadata.h"

namespace kuzu::storage {
class CompressedFlushBuffer {
    std::shared_ptr<CompressionAlg> alg;
    common::PhysicalTypeID dataType;

public:
    CompressedFlushBuffer(std::shared_ptr<CompressionAlg> alg, common::PhysicalTypeID dataType)
        : alg{std::move(alg)}, dataType{dataType} {}
    CompressedFlushBuffer(std::shared_ptr<CompressionAlg> alg, const common::LogicalType& dataType)
        : kuzu::storage::CompressedFlushBuffer(alg, dataType.getPhysicalType()) {}

    CompressedFlushBuffer(const CompressedFlushBuffer& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t bufferSize, BMFileHandle* dataFH,
        common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) const;
};

template<std::floating_point T>
class CompressedFloatFlushBuffer {
    std::shared_ptr<FloatCompression<T>> alg;
    common::PhysicalTypeID dataType;

public:
    CompressedFloatFlushBuffer(std::shared_ptr<FloatCompression<T>> alg,
        common::PhysicalTypeID dataType)
        : alg{std::move(alg)}, dataType{dataType} {}
    CompressedFloatFlushBuffer(std::shared_ptr<FloatCompression<T>> alg,
        const common::LogicalType& dataType)
        : kuzu::storage::CompressedFloatFlushBuffer<T>(alg, dataType.getPhysicalType()) {}

    CompressedFloatFlushBuffer(const CompressedFloatFlushBuffer& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t bufferSize, BMFileHandle* dataFH,
        common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) const;
};

ColumnChunkMetadata uncompressedFlushBuffer(const uint8_t* buffer, uint64_t bufferSize,
    BMFileHandle* dataFH, common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata);

} // namespace kuzu::storage
