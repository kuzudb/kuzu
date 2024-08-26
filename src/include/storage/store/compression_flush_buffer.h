#pragma once

#include "storage/compression/compression.h"
#include "storage/file_handle.h"
#include "storage/store/column_chunk_metadata.h"

namespace kuzu::storage {
class CompressedFlushBuffer {
    std::shared_ptr<CompressionAlg> alg;
    common::PhysicalTypeID dataType;

public:
    CompressedFlushBuffer(std::shared_ptr<CompressionAlg> alg, common::PhysicalTypeID dataType)
        : alg{std::move(alg)}, dataType{dataType} {}
    CompressedFlushBuffer(std::shared_ptr<CompressionAlg> alg, const common::LogicalType& dataType)
        : CompressedFlushBuffer(alg, dataType.getPhysicalType()) {}

    CompressedFlushBuffer(const CompressedFlushBuffer& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t bufferSize, FileHandle* dataFH,
        common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) const;
};

template<std::floating_point T>
class CompressedFloatFlushBuffer {
    std::shared_ptr<CompressionAlg> alg;
    common::PhysicalTypeID dataType;

public:
    CompressedFloatFlushBuffer(std::shared_ptr<CompressionAlg> alg,
        common::PhysicalTypeID dataType);
    CompressedFloatFlushBuffer(std::shared_ptr<CompressionAlg> alg,
        const common::LogicalType& dataType);

    CompressedFloatFlushBuffer(const CompressedFloatFlushBuffer& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t bufferSize, FileHandle* dataFH,
        common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) const;
};

ColumnChunkMetadata uncompressedFlushBuffer(const uint8_t* buffer, uint64_t bufferSize,
    FileHandle* dataFH, common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata);

} // namespace kuzu::storage
