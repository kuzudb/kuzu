#pragma once

#include "storage/table/column_chunk_data.h"
#include "storage/table/segment_scanner.h"
namespace kuzu {
namespace storage {
// Scans all the segments into a single column chunk
struct CombinedSegmentScanner : public SegmentScanner {
    explicit CombinedSegmentScanner(ColumnChunkData& output) : output(output) {}

    void initSegment(common::offset_t, common::offset_t, scan_func_t scanFunc) override {
        scanFunc(output);
    }

    void writeToSegment(ColumnChunkData& data, common::offset_t srcOffset,
        common::offset_t dstOffset) override {
        output.write(&data, srcOffset, dstOffset, 1);
    }

    uint64_t getNumValues() override { return output.getNumValues(); }

    ColumnChunkData& output;
};
} // namespace storage
} // namespace kuzu
