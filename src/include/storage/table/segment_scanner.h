#pragma once

#include <functional>

#include "common/types/types.h"
namespace kuzu {
namespace storage {
class ColumnChunkData;

struct ColumnChunkScanner {
    using scan_func_t = std::function<void(ColumnChunkData& /*outputChunk*/,
        common::offset_t /*offsetInSegment*/, common::offset_t /*length*/)>;

    struct SegmentData {
        std::unique_ptr<ColumnChunkData> segmentData;
        common::offset_t offsetInSegment;
        common::offset_t length;
        scan_func_t scanFunc;
    };

    virtual ~ColumnChunkScanner() {};
    virtual void scanSegment(common::offset_t offsetInSegment, common::offset_t segmentLength,
        scan_func_t scanFunc) = 0;
    virtual void updateScannedValue(ColumnChunkData& data, common::offset_t srcOffset,
        common::offset_t dstOffset) = 0;
    virtual uint64_t getNumValues() = 0;
};
} // namespace storage
} // namespace kuzu
