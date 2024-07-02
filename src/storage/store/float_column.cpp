#include "storage/store/float_column.h"

#include "alp/decode.hpp"
#include "alp/encode.hpp"

namespace kuzu::storage {

template<std::floating_point T>
void FloatColumn::updateStatisticsImpl(ColumnChunkMetadata& metadata, common::offset_t maxIndex,
    const uint8_t* data, common::offset_t dataOffset, size_t numValues,
    const common::NullMask* nullMask) {

    const auto* dataBuffer = reinterpret_cast<const T*>(data);
    for (common::offset_t i = 0; i < numValues; ++i) {
        if (!nullMask || !nullMask->isNull(i)) {
            // TODO handle in place updates for float metadata
            const T& originalValue = dataBuffer[dataOffset + i];
            const int64_t encodedValue = alp::AlpEncode<T>::encode_value(originalValue,
                metadata.compMeta.floatMetadata.fac, metadata.compMeta.floatMetadata.exp);
            const int64_t decodedValue = alp::AlpEncode<T>::encode_value(encodedValue,
                metadata.compMeta.floatMetadata.fac, metadata.compMeta.floatMetadata.exp);

            if (originalValue == decodedValue) {
                // don't include exceptions in statistics computation
                Column::updateMinMaxStatistics(metadata, maxIndex, StorageValue{encodedValue},
                    StorageValue{encodedValue});
            }
        }
    }
}

void FloatColumn::updateStatistics(ColumnChunkMetadata& metadata, common::offset_t maxIndex,
    const uint8_t* data, common::offset_t dataOffset, size_t numValues,
    const common::NullMask* nullMask) {
    if (dataType.getPhysicalType() == common::PhysicalTypeID::DOUBLE) {
        updateStatisticsImpl<double>(metadata, maxIndex, data, dataOffset, numValues, nullMask);
    } else {
        updateStatisticsImpl<float>(metadata, maxIndex, data, dataOffset, numValues, nullMask);
    }
}

template void FloatColumn::updateStatisticsImpl<double>(ColumnChunkMetadata& metadata,
    common::offset_t maxIndex, const uint8_t* data, common::offset_t dataOffset, size_t numValues,
    const common::NullMask* nullMask);
template void FloatColumn::updateStatisticsImpl<float>(ColumnChunkMetadata& metadata,
    common::offset_t maxIndex, const uint8_t* data, common::offset_t dataOffset, size_t numValues,
    const common::NullMask* nullMask);

} // namespace kuzu::storage
