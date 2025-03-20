#pragma once

#include "storage/compression/compression.h"
namespace kuzu::storage {

struct KUZU_API ColumnChunkStats {
    std::optional<StorageValue> max;
    std::optional<StorageValue> min;

    void update(std::optional<StorageValue> min, std::optional<StorageValue> max,
        common::PhysicalTypeID dataType);
    void update(StorageValue val, common::PhysicalTypeID dataType);
    void update(uint8_t* data, uint64_t offset, uint64_t numValues,
        const common::NullMask* nullMask, common::PhysicalTypeID physicalType);
    void reset();
};

struct MergedColumnChunkStats {
    MergedColumnChunkStats(ColumnChunkStats stats, bool guaranteedNoNulls, bool guaranteedAllNulls)
        : stats(stats), guaranteedNoNulls(guaranteedNoNulls),
          guaranteedAllNulls(guaranteedAllNulls) {}

    ColumnChunkStats stats;
    bool guaranteedNoNulls;
    bool guaranteedAllNulls;

    void merge(const MergedColumnChunkStats& o, common::PhysicalTypeID dataType);
};

} // namespace kuzu::storage
