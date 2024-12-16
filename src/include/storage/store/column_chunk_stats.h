#pragma once

#include "storage/compression/compression.h"
namespace kuzu::storage {

struct ColumnChunkStats {
    std::optional<StorageValue> max;
    std::optional<StorageValue> min;
    bool mayHaveNulls = false;

    void update(std::optional<StorageValue> min, std::optional<StorageValue> max, bool mayHaveNulls,
        common::PhysicalTypeID dataType);
    void update(StorageValue val, common::PhysicalTypeID dataType);
    void update(uint8_t* data, uint64_t offset, uint64_t numValues, bool mayHaveNulls,
        common::PhysicalTypeID physicalType);
    void reset();

    void updateMayHaveNulls(bool newMayHaveNulls);
};

} // namespace kuzu::storage
