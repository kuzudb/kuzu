#pragma once

#include <cstddef>
#include <cstdint>

#include "common/assert.h"
#include "common/cast.h"
#include "common/types/types.h"
#include "storage/buffer_manager/spill_result.h"
#include "storage/table/column_chunk_data.h"
#include "storage/table/update_info.h"

namespace kuzu {
namespace storage {
class PageAllocator;
class MemoryManager;
class Column;

struct ChunkCheckpointState {
    std::unique_ptr<ColumnChunkData> chunkData;
    // Start offset in the column chunk of the beginning of the chunk data
    common::row_idx_t startRow;
    common::length_t numRows;

    ChunkCheckpointState(std::unique_ptr<ColumnChunkData> chunkData, common::row_idx_t startRow,
        common::length_t numRows)
        : chunkData{std::move(chunkData)}, startRow{startRow}, numRows{numRows} {}
};

struct SegmentCheckpointState {
    const ColumnChunkData& chunkData;
    common::row_idx_t startRowInData;
    common::row_idx_t offsetInSegment;
    common::row_idx_t numRows;
};

// dstOffset starts from 0 and is the offset in the output data for a given segment
//  (it increases by lengthInSegment for each segment)
// Returns the total number of values scanned (input length can be longer than the available
// values)
template<class SegmentView,
    std::invocable<SegmentView&, common::offset_t /*offsetInSegment*/,
        common::offset_t /*lengthInSegment*/, common::offset_t /*dstOffset*/>
        Func>
common::offset_t genericRangeSegments(std::span<SegmentView> segments,
    common::offset_t offsetInChunk, common::length_t length, Func func) {
    // TODO(bmwinger): try binary search (might only make a difference for a very large number
    // of segments)
    auto segment = segments.begin();
    auto offsetInSegment = offsetInChunk;
    while ((**segment).getNumValues() <= offsetInSegment) {
        offsetInSegment -= (**segment).getNumValues();
        KU_ASSERT(segment < segments.end() - 1);
        segment++;
    }
    common::offset_t lengthScanned = 0;
    common::offset_t dstOffset = 0;
    while (lengthScanned < length && segment != segments.end()) {
        KU_ASSERT((**segment).getNumValues() > offsetInSegment);
        auto lengthInSegment =
            std::min(length - lengthScanned, (**segment).getNumValues() - offsetInSegment);
        func(*segment, offsetInSegment, lengthInSegment, dstOffset);
        lengthScanned += lengthInSegment;
        segment++;
        dstOffset += lengthInSegment;
        offsetInSegment = 0;
    }
    return dstOffset;
}

class ColumnChunk;
struct ColumnCheckpointState {
    ColumnChunkData& persistentData;
    std::vector<SegmentCheckpointState> segmentCheckpointStates;
    common::row_idx_t endRowIdxToWrite;

    ColumnCheckpointState(ColumnChunkData& persistentData,
        std::vector<SegmentCheckpointState> segmentCheckpointStates)
        : persistentData{persistentData},
          segmentCheckpointStates{std::move(segmentCheckpointStates)}, endRowIdxToWrite{0} {
        for (const auto& chunkCheckpointState : this->segmentCheckpointStates) {
            const auto endRowIdx =
                chunkCheckpointState.offsetInSegment + chunkCheckpointState.numRows;
            if (endRowIdx > endRowIdxToWrite) {
                endRowIdxToWrite = endRowIdx;
            }
        }
    }
};

struct ChunkState {
    const Column* column;
    std::vector<SegmentState> segmentStates;

    void reclaimAllocatedPages(PageAllocator& pageAllocator) const;

    const SegmentState* findSegment(common::offset_t offsetInChunk,
        common::offset_t& offsetInSegment) const {
        offsetInSegment = offsetInChunk;
        for (const auto& segmentState : segmentStates) {
            if (offsetInSegment < segmentState.metadata.numValues) {
                return &segmentState;
            }
            offsetInSegment -= segmentState.metadata.numValues;
        }
        return nullptr;
    }

    // dstOffset starts from 0 and is the offset in the output data for a given segment
    //  (it increases by lengthInSegment for each segment)
    // Returns the total number of values scanned (input length can be longer than the available
    // values)
    template<std::invocable<SegmentState&, common::offset_t /*offsetInSegment*/,
        common::offset_t /*lengthInSegment*/, common::offset_t /*dstOffset*/>
            Func>
    common::offset_t rangeSegments(common::offset_t offsetInChunk, common::length_t length,
        Func func) {
        return genericRangeSegments(std::span(segmentStates), offsetInChunk, length, func);
    }

    // TODO(bmwinger): the above function should be const and only isn't because of ALP exception
    // chunk modifications. The SegmentState& should also be const for the same reason
    template<std::invocable<SegmentState&, common::offset_t /*offsetInSegment*/,
        common::offset_t /*lengthInSegment*/, common::offset_t /*dstOffset*/>
            Func>
    common::offset_t rangeSegments(common::offset_t offsetInChunk, common::length_t length,
        Func func) const {
        return const_cast<ChunkState*>(this)->rangeSegments(offsetInChunk, length, func);
    }
};

class ColumnChunk {
public:
    ColumnChunk(MemoryManager& mm, common::LogicalType&& dataType, uint64_t capacity,
        bool enableCompression, ResidencyState residencyState, bool initializeToZero = true);
    ColumnChunk(MemoryManager& mm, common::LogicalType&& dataType, bool enableCompression,
        ColumnChunkMetadata metadata);
    ColumnChunk(MemoryManager& mm, bool enableCompression, std::unique_ptr<ColumnChunkData> data);
    ColumnChunk(MemoryManager& mm, bool enableCompression,
        std::vector<std::unique_ptr<ColumnChunkData>> segments);

    void initializeScanState(ChunkState& state, const Column* column) const;
    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::ValueVector& output, common::offset_t offsetInChunk, common::length_t length) const;
    template<ResidencyState SCAN_RESIDENCY_STATE>
    void scanCommitted(const transaction::Transaction* transaction, ChunkState& chunkState,
        ColumnChunkData& output, common::row_idx_t startRow = 0,
        common::row_idx_t numRows = common::INVALID_ROW_IDX) const;
    void lookup(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t rowInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const;
    void update(const transaction::Transaction* transaction, common::offset_t offsetInChunk,
        const common::ValueVector& values);

    uint64_t getEstimatedMemoryUsage() const {
        if (getResidencyState() == ResidencyState::ON_DISK) {
            return 0;
        }
        uint64_t memUsage = 0;
        for (auto& segment : data) {
            memUsage += segment->getEstimatedMemoryUsage();
        }
        return memUsage;
    }
    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<ColumnChunk> deserialize(MemoryManager& mm, common::Deserializer& deSer);

    uint64_t getNumValues() const {
        uint64_t numValues = 0;
        for (const auto& chunk : data) {
            numValues += chunk->getNumValues();
        }
        return numValues;
    }
    uint64_t getCapacity() const {
        uint64_t capacity = 0;
        for (const auto& chunk : data) {
            capacity += chunk->getCapacity();
        }
        return capacity;
    }
    void truncate(uint64_t numValues) {
        uint64_t seenValues = 0;
        uint64_t seenSegments = 0;
        for (auto& segment : data) {
            seenSegments++;
            if (seenValues + segment->getNumValues() < numValues) {
                seenValues += segment->getNumValues();
            } else {
                segment->setNumValues(numValues - seenValues);
                break;
            }
        }
        data.resize(seenSegments);
    }

    common::row_idx_t getNumUpdatedRows(const transaction::Transaction* transaction) const;

    std::pair<std::unique_ptr<ColumnChunkData>, std::unique_ptr<ColumnChunkData>> scanUpdates(
        const transaction::Transaction* transaction) const;

    // TODO(bmwinger): Segments could probably share a single datatype
    common::LogicalType& getDataType() { return data.front()->getDataType(); }
    const common::LogicalType& getDataType() const { return data.front()->getDataType(); }
    bool isCompressionEnabled() const { return enableCompression; }

    ResidencyState getResidencyState() const {
        auto state = data.front()->getResidencyState();
        RUNTIME_CHECK(for (auto& chunk : data) { KU_ASSERT(chunk->getResidencyState() == state); });
        return state;
    }
    bool hasUpdates() const { return updateInfo != nullptr; }
    bool hasUpdates(const transaction::Transaction* transaction, common::row_idx_t startRow,
        common::length_t numRows) const;
    void resetUpdateInfo() {
        if (updateInfo) {
            updateInfo.reset();
        }
    }

    void loadFromDisk() {
        for (auto& segment : data) {
            segment->loadFromDisk();
        }
    }
    SpillResult spillToDisk() {
        SpillResult spilled;
        for (auto& segment : data) {
            spilled += segment->spillToDisk();
        }
        return spilled;
    }

    MergedColumnChunkStats getMergedColumnChunkStats(
        const transaction::Transaction* transaction) const;

    void reclaimStorage(PageAllocator& pageAllocator) const;

    void append(common::ValueVector* vector, const common::SelectionView& selView);
    void append(const ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    void append(const ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    template<typename T, std::invocable<T&, uint64_t> Func>
    void mapValues(Func func, uint64_t startOffset = 0, uint64_t endOffset = UINT64_MAX) {
        uint64_t startPos = 0;
        for (const auto& segment : data) {
            auto* segmentData = segment->getData<T>();
            auto startOffsetInSegment =
                std::max(std::min(segment->getNumValues(), startOffset), uint64_t{0});
            for (size_t i = startOffsetInSegment;
                 i < segment->getNumValues() && startPos + i < endOffset; i++) {
                func(segmentData[i], startPos + i);
            }
            startPos += segment->getNumValues();
            if (startPos >= endOffset) {
                break;
            }
        }
    }

    template<typename T>
    T getValue(common::offset_t pos) const {
        KU_ASSERT(pos < getNumValues());
        for (const auto& segment : data) {
            if (segment->getNumValues() > pos) {
                return segment->getValue<T>(pos);
            } else {
                pos -= segment->getNumValues();
            }
        }
        KU_UNREACHABLE;
    }

    template<typename T>
    void setValue(T val, common::offset_t pos) const {
        KU_ASSERT(pos < getCapacity());
        for (const auto& segment : data) {
            if (segment->getCapacity() > pos) {
                segment->setValue<T>(val, pos);
                return;
            } else {
                pos -= segment->getNumValues();
            }
        }
        KU_UNREACHABLE;
    }

    void flush(PageAllocator& pageAllocator) {
        for (auto& segment : data) {
            segment->flush(pageAllocator);
        }
    }

    std::unique_ptr<ColumnChunk> flushAsNewColumnChunk(PageAllocator& pageAllocator) const;

    void populateWithDefaultVal(evaluator::ExpressionEvaluator& defaultEvaluator,
        uint64_t& numValues_, ColumnStats* newColumnStats) {
        KU_ASSERT(data.size() == 1 && data.back()->getNumValues() == 0);
        data.back()->populateWithDefaultVal(defaultEvaluator, numValues_, newColumnStats);
    }

    void finalize() {
        for (auto& segment : data) {
            segment->finalize();
        }
    }

    // TODO(bmwinger): This is not ideal; it's just a workaround for storage_info
    // We should either provide a way for ColumnChunk to provide its own details about the
    // storage structure, or maybe change the type of data to allow us to directly return a
    // std::span<const ColumnChunkData> to get read-only info about the segments efficiently
    std::vector<const ColumnChunkData*> getSegments() const {
        std::vector<const ColumnChunkData*> segments;
        for (const auto& segment : data) {
            segments.push_back(segment.get());
        }
        return segments;
    }

    void checkpoint(Column& column, std::vector<ChunkCheckpointState>&& chunkCheckpointStates,
        PageAllocator& pageAllocator);

    void write(Column& column, ChunkState& state, common::offset_t dstOffset,
        const ColumnChunkData& dataToWrite, common::offset_t srcOffset, common::length_t numValues);

    void syncNumValues() {
        for (auto& segment : data) {
            segment->syncNumValues();
        }
    }

    void setTableID(common::table_id_t tableID) {
        for (const auto& segment : data) {
            auto internalIDSegment = common::ku_dynamic_cast<InternalIDChunkData*>(segment.get());
            internalIDSegment->setTableID(tableID);
        }
    }

private:
    void scanCommittedUpdates(const transaction::Transaction* transaction, ColumnChunkData& output,
        common::offset_t startOffsetInOutput, common::row_idx_t startRowScanned,
        common::row_idx_t numRows) const;

    template<typename Func>
    void rangeSegments(common::offset_t offsetInChunk, common::length_t length, Func func) const {
        genericRangeSegments(std::span(data), offsetInChunk, length, func);
    }

private:
    MemoryManager& mm;
    // TODO(Guodong): This field should be removed. Ideally it shouldn't be cached anywhere in
    // storage structures, instead should be fed into functions needed from ClientContext
    // dbConfig.
    bool enableCompression;
    std::vector<std::unique_ptr<ColumnChunkData>> data;
    // Update versions.
    std::unique_ptr<UpdateInfo> updateInfo;
};

} // namespace storage
} // namespace kuzu
