#include "storage/store/column_chunk_stats.h"

#include "common/type_utils.h"

namespace kuzu {
namespace storage {

void ColumnChunkStats::update(uint8_t* data, uint64_t offset, uint64_t numValues,
    common::PhysicalTypeID physicalType) {
    const bool isStorageValueType =
        common::TypeUtils::visit(physicalType, []<typename T>(T) { return StorageValueType<T>; });
    if (isStorageValueType) {
        auto [minVal, maxVal] =
            getMinMaxStorageValue(data, offset, numValues, physicalType, nullptr);
        update(minVal, maxVal, physicalType);
    }
}

void ColumnChunkStats::update(std::optional<StorageValue> newMin,
    std::optional<StorageValue> newMax, common::PhysicalTypeID dataType) {
    if (!min.has_value() || (newMin.has_value() && min->gt(*newMin, dataType))) {
        min = newMin;
    }
    if (!max.has_value() || (newMax.has_value() && newMax->gt(*max, dataType))) {
        max = newMax;
    }
}

void ColumnChunkStats::update(StorageValue val, common::PhysicalTypeID dataType) {
    update(val, val, dataType);
}

void ColumnChunkStats::reset() {
    *this = {};
}

} // namespace storage
} // namespace kuzu
