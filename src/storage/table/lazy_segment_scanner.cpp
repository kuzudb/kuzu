#include "storage/table/lazy_segment_scanner.h"

namespace kuzu::storage {
void LazySegmentScanner::Iterator::advance(common::offset_t n) {
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

void LazySegmentScanner::scanSegment(common::offset_t offsetInSegment,
    common::offset_t segmentLength, scan_func_t newScanFunc) {
    segments.emplace_back(nullptr, offsetInSegment, segmentLength, std::move(newScanFunc));
    numValues += segmentLength;
}

void LazySegmentScanner::applyCommittedUpdates(const UpdateInfo& updateInfo,
    const transaction::Transaction* transaction, common::offset_t startRow,
    common::offset_t numRows) {
    KU_ASSERT(numRows == numValues);
    rangeSegments(begin(), numRows,
        [&](auto& segment, common::offset_t, common::offset_t lengthInSegment,
            common::offset_t offsetInChunk) {
            scanSegmentIfNeeded(segment);
            updateInfo.scanCommitted(transaction, *segment.segmentData, 0, startRow + offsetInChunk,
                lengthInSegment);
        });
}

void LazySegmentScanner::scanSegmentIfNeeded(SegmentData& segment) {
    if (segment.segmentData == nullptr) {
        segment.segmentData = ColumnChunkFactory::createColumnChunkData(mm, columnType.copy(),
            enableCompression, segment.length, ResidencyState::IN_MEMORY);

        segment.scanFunc(*segment.segmentData, segment.startOffsetInSegment, segment.length);
    }
}
} // namespace kuzu::storage
