#pragma once

#include "column.h"

namespace kuzu {
namespace storage {

class FloatColumn final : public Column {
private:
    void updateStatistics(ColumnChunkMetadata& metadata, common::offset_t maxIndex,
        const uint8_t* data, common::offset_t dataOffset, size_t numValues,
        const common::NullMask* nullMask = nullptr) override;

    template<std::floating_point T>
    void updateStatisticsImpl(ColumnChunkMetadata& metadata, common::offset_t maxIndex,
        const uint8_t* data, common::offset_t dataOffset, size_t numValues,
        const common::NullMask* nullMask = nullptr);
};

} // namespace storage
} // namespace kuzu
