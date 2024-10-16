#pragma once

#include "common/types/types.h"
#include "storage/compression/compression.h"

namespace kuzu::storage {
struct ColumnChunkMetadata {
    common::page_idx_t pageIdx;
    common::page_idx_t numPages;
    uint64_t numValues;
    CompressionMetadata compMeta;

    void serialize(common::Serializer& serializer) const;
    static ColumnChunkMetadata deserialize(common::Deserializer& deserializer);

    // TODO(Guodong): Delete copy constructor.
    ColumnChunkMetadata()
        : pageIdx{common::INVALID_PAGE_IDX}, numPages{0}, numValues{0},
          compMeta(StorageValue(), StorageValue(), CompressionType::CONSTANT) {}
    ColumnChunkMetadata(common::page_idx_t pageIdx, common::page_idx_t numPages, uint64_t numValues,
        const CompressionMetadata& compMeta)
        : pageIdx(pageIdx), numPages(numPages), numValues(numValues), compMeta(compMeta) {}
};

class GetCompressionMetadata {
    std::shared_ptr<CompressionAlg> alg;
    const common::LogicalType& dataType;

public:
    GetCompressionMetadata(std::shared_ptr<CompressionAlg> alg, const common::LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    GetCompressionMetadata(const GetCompressionMetadata& other) = default;

    ColumnChunkMetadata operator()(std::span<const uint8_t> buffer, uint64_t capacity,
        uint64_t numValues, StorageValue min, StorageValue max) const;
};

class GetBitpackingMetadata {
    std::shared_ptr<CompressionAlg> alg;
    const common::LogicalType& dataType;

public:
    GetBitpackingMetadata(std::shared_ptr<CompressionAlg> alg, const common::LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    GetBitpackingMetadata(const GetBitpackingMetadata& other) = default;

    ColumnChunkMetadata operator()(std::span<const uint8_t> buffer, uint64_t capacity,
        uint64_t numValues, StorageValue min, StorageValue max);
};

template<std::floating_point T>
class GetFloatCompressionMetadata {
    std::shared_ptr<CompressionAlg> alg;
    const common::LogicalType& dataType;

public:
    GetFloatCompressionMetadata(std::shared_ptr<CompressionAlg> alg,
        const common::LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    GetFloatCompressionMetadata(const GetFloatCompressionMetadata& other) = default;

    ColumnChunkMetadata operator()(std::span<const uint8_t> buffer, uint64_t capacity,
        uint64_t numValues, StorageValue min, StorageValue max);
};

ColumnChunkMetadata uncompressedGetMetadata(std::span<const uint8_t> buffer, uint64_t capacity,
    uint64_t numValues, StorageValue min, StorageValue max);

ColumnChunkMetadata booleanGetMetadata(std::span<const uint8_t> buffer, uint64_t capacity,
    uint64_t numValues, StorageValue min, StorageValue max);
} // namespace kuzu::storage
