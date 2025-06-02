#pragma once

#include <cstddef>
#include <cstdint>

#include "common/types/types.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/update_info.h"

namespace kuzu {
namespace storage {
class MemoryManager;
class Column;

struct ChunkCheckpointState {
    std::unique_ptr<ColumnChunkData> chunkData;
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

class ColumnChunk {
public:
    ColumnChunk(MemoryManager& memoryManager, common::LogicalType&& dataType, uint64_t capacity,
        bool enableCompression, ResidencyState residencyState, bool initializeToZero = true);
    ColumnChunk(MemoryManager& memoryManager, common::LogicalType&& dataType,
        bool enableCompression, ColumnChunkMetadata metadata);
    ColumnChunk(bool enableCompression, std::unique_ptr<ColumnChunkData> data);
    ColumnChunk(bool enableCompression, std::vector<std::unique_ptr<ColumnChunkData>> segments);

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
    static std::unique_ptr<ColumnChunk> deserialize(MemoryManager& memoryManager,
        common::Deserializer& deSer);

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
    void setNumValues(const uint64_t numValues) const {
        // TODO(bmwinger): Not sure how to handle this. Probably rework the caller
        // Only used for rollbackInsert with an argument of 0. Not sure what the best behaviour
        // would be there; should we clear all but one and set that to 0? or set them all to 0?
        KU_ASSERT(numValues == 0);
        for (auto& segment : data) {
            segment->setNumValues(numValues);
        }
    }

    common::row_idx_t getNumUpdatedRows(const transaction::Transaction* transaction) const;

    std::pair<std::unique_ptr<ColumnChunkData>, std::unique_ptr<ColumnChunkData>> scanUpdates(
        const transaction::Transaction* transaction) const;

    // TODO: Segments could probably share a single datatype
    common::LogicalType& getDataType() { return data.front()->getDataType(); }
    const common::LogicalType& getDataType() const { return data.front()->getDataType(); }
    bool isCompressionEnabled() const { return enableCompression; }

    ResidencyState getResidencyState() const {
        // TODO: handle per-segment
        return data.front()->getResidencyState();
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
    uint64_t spillToDisk() {
        uint64_t spilled = 0;
        for (auto& segment : data) {
            spilled += segment->spillToDisk();
        }
        return spilled;
    }

    MergedColumnChunkStats getMergedColumnChunkStats(
        const transaction::Transaction* transaction) const;

    void reclaimStorage(FileHandle& dataFH);

    void append(common::ValueVector* vector, const common::SelectionView& selView);
    void append(const ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    void append(const ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    template<typename T, class Func>
    void mapValues(Func func) {
        uint64_t startPos = 0;
        for (const auto& segment : data) {
            auto* segmentData = segment->getData<T>();
            for (size_t i = 0; i < segment->getNumValues(); i++) {
                func(segmentData[i], startPos + i);
            }
            startPos += segment->getNumValues();
        }
    }

    template<typename T, class Func>
    void mapValues(uint64_t startOffset, Func func) {
        uint64_t startPos = 0;
        for (const auto& segment : data) {
            auto* segmentData = segment->getData<T>();
            auto startOffsetInSegment =
                std::max(std::min(segment->getNumValues(), startOffset), uint64_t{0});
            for (size_t i = startOffsetInSegment; i < segment->getNumValues(); i++) {
                func(segmentData[i], startPos + i);
            }
            startPos += segment->getNumValues();
        }
    }

    template<typename T, class Func>
    void mapValues(uint64_t startOffset, uint64_t endOffset, Func func) {
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

    void flush(FileHandle& dataFH) {
        for (auto& segment : data) {
            segment->flush(dataFH);
        }
    }

    std::unique_ptr<ColumnChunk> flushAsNewColumnChunk(FileHandle& dataFH) const;

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
    // We should either provide a way for ColumnChunk to provide its own details about the storage
    // structure, or maybe change the type of data to allow us to directly return a std::span<const
    // ColumnChunkData> to get read-only info about the segments efficiently
    std::vector<const ColumnChunkData*> getSegments() const {
        std::vector<const ColumnChunkData*> segments;
        for (const auto& segment : data) {
            segments.push_back(segment.get());
        }
        return segments;
    }

    void checkpoint(Column& column, std::vector<ChunkCheckpointState>&& chunkCheckpointStates);

    void write(Column& column, ChunkState& state, common::offset_t dstOffset,
        const ColumnChunkData& dataToWrite, common::offset_t srcOffset, common::length_t numValues);

    void syncNumValues() {
        for (auto& segment : data) {
            segment->syncNumValues();
        }
    }

private:
    void scanCommittedUpdates(const transaction::Transaction* transaction, ColumnChunkData& output,
        common::offset_t startOffsetInOutput, common::row_idx_t startRowScanned,
        common::row_idx_t numRows) const;

    template<typename Func>
    void rangeSegments(common::offset_t offsetInChunk, common::length_t length, Func func) const {
        // TODO(bmwinger): try binary search (might only make a difference for a very large number
        // of segments)
        auto segment = data.begin();
        auto offsetInSegment = offsetInChunk;
        while (segment->get()->getNumValues() < offsetInSegment) {
            offsetInSegment -= segment->get()->getNumValues();
            segment++;
        }
        uint64_t lengthScanned = 0;
        while (lengthScanned < length) {
            auto lengthInSegment = std::min(length, segment->get()->getNumValues());
            func(*segment->get(), offsetInSegment, lengthInSegment, lengthScanned);
            lengthScanned += lengthInSegment;
            segment++;
        }
    }

private:
    // TODO(Guodong): This field should be removed. Ideally it shouldn't be cached anywhere in
    // storage structures, instead should be fed into functions needed from ClientContext dbConfig.
    bool enableCompression;
    std::vector<std::unique_ptr<ColumnChunkData>> data;
    // Update versions.
    std::unique_ptr<UpdateInfo> updateInfo;
};

} // namespace storage
} // namespace kuzu
