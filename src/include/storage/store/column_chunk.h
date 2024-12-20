#pragma once

#include "storage/store/column_chunk_data.h"
#include "storage/store/update_info.h"

namespace kuzu {
namespace storage {
class MemoryManager;

struct ChunkCheckpointState {
    std::unique_ptr<ColumnChunkData> chunkData;
    common::row_idx_t startRow;
    common::length_t numRows;

    ChunkCheckpointState(std::unique_ptr<ColumnChunkData> chunkData, common::row_idx_t startRow,
        common::length_t numRows)
        : chunkData{std::move(chunkData)}, startRow{startRow}, numRows{numRows} {}
};

struct ColumnCheckpointState {
    ColumnChunkData& persistentData;
    std::vector<ChunkCheckpointState> chunkCheckpointStates;
    common::row_idx_t maxRowIdxToWrite;

    ColumnCheckpointState(ColumnChunkData& persistentData,
        std::vector<ChunkCheckpointState> chunkCheckpointStates)
        : persistentData{persistentData}, chunkCheckpointStates{std::move(chunkCheckpointStates)},
          maxRowIdxToWrite{0} {
        for (const auto& chunkCheckpointState : this->chunkCheckpointStates) {
            const auto endRowIdx = chunkCheckpointState.startRow + chunkCheckpointState.numRows;
            if (endRowIdx > maxRowIdxToWrite) {
                maxRowIdxToWrite = endRowIdx;
            }
        }
    }
};

class ColumnChunk {
public:
    // TODO(bmwinger): the dataType reference is copied when passing it to the ColumnChunkData
    // It would be better to take it by value so that the caller can choose to either move or copy
    // it
    ColumnChunk(MemoryManager& memoryManager, const common::LogicalType& dataType,
        uint64_t capacity, bool enableCompression, ResidencyState residencyState,
        bool initializeToZero = true);
    ColumnChunk(MemoryManager& memoryManager, const common::LogicalType& dataType,
        bool enableCompression, ColumnChunkMetadata metadata);
    ColumnChunk(bool enableCompression, std::unique_ptr<ColumnChunkData> data);

    void initializeScanState(ChunkState& state, const Column* column) const;
    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::ValueVector& output, common::offset_t offsetInChunk, common::length_t length) const;
    template<ResidencyState SCAN_RESIDENCY_STATE>
    void scanCommitted(const transaction::Transaction* transaction, ChunkState& chunkState,
        ColumnChunk& output, common::row_idx_t startRow = 0,
        common::row_idx_t numRows = common::INVALID_ROW_IDX) const;
    void lookup(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t rowInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const;
    void update(const transaction::Transaction* transaction, common::offset_t offsetInChunk,
        const common::ValueVector& values);

    uint64_t getEstimatedMemoryUsage() const {
        return getResidencyState() == ResidencyState::ON_DISK ? 0 : data->getEstimatedMemoryUsage();
    }
    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<ColumnChunk> deserialize(MemoryManager& memoryManager,
        common::Deserializer& deSer);

    uint64_t getNumValues() const { return data->getNumValues(); }
    void setNumValues(const uint64_t numValues) const { data->setNumValues(numValues); }

    common::row_idx_t getNumUpdatedRows(const transaction::Transaction* transaction) const;

    std::pair<std::unique_ptr<ColumnChunk>, std::unique_ptr<ColumnChunk>> scanUpdates(
        const transaction::Transaction* transaction) const;

    void setData(std::unique_ptr<ColumnChunkData> data) { this->data = std::move(data); }
    // Functions to access the in memory data.
    ColumnChunkData& getData() const { return *data; }
    const ColumnChunkData& getConstData() const { return *data; }
    std::unique_ptr<ColumnChunkData> moveData() { return std::move(data); }

    common::LogicalType& getDataType() { return data->getDataType(); }
    const common::LogicalType& getDataType() const { return data->getDataType(); }
    bool isCompressionEnabled() const { return enableCompression; }

    ResidencyState getResidencyState() const { return data->getResidencyState(); }
    bool hasUpdates() const { return updateInfo != nullptr; }
    bool hasUpdates(const transaction::Transaction* transaction, common::row_idx_t startRow,
        common::length_t numRows) const;
    // These functions should only work on in-memory and temporary column chunks.
    void resetToEmpty() const { data->resetToEmpty(); }
    void resetToAllNull() const { data->resetToAllNull(); }
    void resize(uint64_t newSize) const { data->resize(newSize); }
    void resetUpdateInfo() {
        if (updateInfo) {
            updateInfo.reset();
        }
    }

    void loadFromDisk() { data->loadFromDisk(); }
    uint64_t spillToDisk() { return data->spillToDisk(); }

    MergedColumnChunkStats getMergedColumnChunkStats(
        const transaction::Transaction* transaction) const;

private:
    void scanCommittedUpdates(const transaction::Transaction* transaction, ColumnChunkData& output,
        common::offset_t startOffsetInOutput, common::row_idx_t startRowScanned,
        common::row_idx_t numRows) const;

private:
    // TODO(Guodong): This field should be removed. Ideally it shouldn't be cached anywhere in
    // storage structures, instead should be fed into functions needed from ClientContext dbConfig.
    bool enableCompression;
    std::unique_ptr<ColumnChunkData> data;
    // Update versions.
    std::unique_ptr<UpdateInfo> updateInfo;
};

} // namespace storage
} // namespace kuzu
