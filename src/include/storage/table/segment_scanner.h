#pragma once

#include <functional>

#include "common/types/types.h"
namespace kuzu {
namespace storage {
class ColumnChunkData;

struct SegmentScanner {
    using scan_func_t = std::function<void(ColumnChunkData& /*outputChunk*/)>;
    using range_func_t =
        std::function<void(common::idx_t /*segmentIdx*/, common::offset_t /*offsetInSegment*/,
            common::offset_t /*lengthInSegment*/, common::offset_t /*dstOffset*/)>;

    struct SegmentData {
        std::unique_ptr<ColumnChunkData> segmentData;
        common::offset_t offsetInChunk;
        common::offset_t length;
        scan_func_t scanFunc;
    };

    virtual ~SegmentScanner() {};
    virtual void initSegment(common::offset_t dstOffset, common::offset_t segmentLength,
        scan_func_t scanFunc) = 0;
    virtual void writeToSegment(ColumnChunkData& data, common::offset_t srcOffset,
        common::offset_t dstOffset) = 0;
    virtual uint64_t getNumValues() = 0;
};
} // namespace storage
} // namespace kuzu
