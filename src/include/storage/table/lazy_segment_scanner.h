#pragma once

#include "storage/table/column_chunk.h"
#include "storage/table/segment_scanner.h"
namespace kuzu {
namespace storage {
// Separately scans each segment in a column chunk
// Avoids scanning a segment unless a call to updateScannedValue() is made for the current segment
struct LazySegmentScanner : public ColumnChunkScanner {
    LazySegmentScanner(MemoryManager& mm, common::LogicalType columnType, bool enableCompression)
        : numValues(0), mm(mm), columnType(std::move(columnType)),
          enableCompression(enableCompression) {}

    struct Iterator {
        common::offset_t segmentIdx;
        common::offset_t offsetInSegment;
        LazySegmentScanner& segmentScanner;

        void advance(common::offset_t n) {
            segmentScanner.rangeSegments(*this, n,
                [this](auto& segmentData, auto, auto lengthInSegment, auto) {
                    KU_ASSERT(segmentData.length > offsetInSegment);
                    if (segmentData.length - offsetInSegment == lengthInSegment) {
                        ++segmentIdx;
                        offsetInSegment = 0;
                    } else {
                        offsetInSegment += lengthInSegment;
                    }
                });
        }

        void operator++() { advance(1); }

        ColumnChunkScanner::SegmentData& operator*() const {
            KU_ASSERT(segmentIdx < segmentScanner.segments.size() &&
                      offsetInSegment < segmentScanner.segments[segmentIdx].length);
            return segmentScanner.segments[segmentIdx];
        }

        ColumnChunkScanner::SegmentData* operator->() const { return &*(*this); }
    };

    void scanSegment(common::offset_t offsetInSegment, common::offset_t segmentLength,
        scan_func_t newScanFunc) override {
        segments.emplace_back(nullptr, offsetInSegment, segmentLength, std::move(newScanFunc));
        numValues += segmentLength;
    }

    void updateScannedValue(ColumnChunkData& data, common::offset_t srcOffset,
        common::offset_t dstOffset) override {
        KU_ASSERT(dstOffset < numValues);
        auto segmentIdx = 0;
        while (dstOffset >= segments[segmentIdx].length) {
            dstOffset -= segments[segmentIdx].length;
            ++segmentIdx;
        }
        initScannedSegmentIfNeeded(segmentIdx);
        auto& segment = segments[segmentIdx];
        segment.segmentData->write(&data, srcOffset, dstOffset, 1);
    }

    uint64_t getNumValues() override { return numValues; }

    void initScannedSegmentIfNeeded(common::idx_t segmentIdx) {
        auto& currentSegment = segments[segmentIdx];
        if (currentSegment.segmentData == nullptr) {
            currentSegment.segmentData =
                ColumnChunkFactory::createColumnChunkData(mm, columnType.copy(), enableCompression,
                    currentSegment.length, ResidencyState::IN_MEMORY);

            currentSegment.scanFunc(*currentSegment.segmentData,
                currentSegment.startOffsetInSegment, currentSegment.length);
        }
    }

    template<std::invocable<SegmentData&, common::offset_t /*offsetInSegment*/,
        common::offset_t /*lengthInSegment*/, common::offset_t /*dstOffset*/>
            Func>
    void rangeSegments(Iterator startIt, common::length_t length, Func func) const {
        auto segmentSpan = std::span(segments);
        genericRangeSegmentsFromIt(segmentSpan, segmentSpan.begin() + startIt.segmentIdx,
            startIt.offsetInSegment, length, func);
    }

    std::vector<SegmentData> segments;

    uint64_t numValues;

    MemoryManager& mm;
    common::LogicalType columnType;
    bool enableCompression;
};
} // namespace storage
} // namespace kuzu
