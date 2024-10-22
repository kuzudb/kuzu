#pragma once

#include "storage/compression/compression.h"
namespace kuzu::storage {

struct ColumnChunkStats {
    std::optional<StorageValue> max;
    std::optional<StorageValue> min;

    void update(std::optional<StorageValue> min, std::optional<StorageValue> max,
        common::PhysicalTypeID dataType);
    void update(StorageValue val, common::PhysicalTypeID dataType);
    void update(uint8_t* data, uint64_t offset, uint64_t numValues,
        common::PhysicalTypeID physicalType);
    void reset();
};

} // namespace kuzu::storage
