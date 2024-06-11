#pragma once

#include "storage/store/update_info.h"

namespace kuzu {
namespace storage {

class ColumnChunk {
public:
    ColumnChunk(const common::LogicalType& dataType, uint64_t capacity, bool enableCompression,
        ResidencyState type);
    ColumnChunk(const common::LogicalType& dataType, bool enableCompression,
        ColumnChunkMetadata metadata);
    ColumnChunk(bool enableCompression, std::unique_ptr<ColumnChunkData> data);

    void initializeScanState(ChunkState& state) const;
    void scan(transaction::Transaction* transaction, ChunkState& state, common::ValueVector& nodeID,
        common::ValueVector& output, common::offset_t offsetInChunk, common::length_t length) const;
    void scanInMemCommitted(ColumnChunkData& output) const;
    void lookup(transaction::Transaction* transaction, common::offset_t offsetInChunk,
        common::ValueVector& output, common::sel_t posInOutputVector) const;
    void update(transaction::Transaction* transaction, common::offset_t offsetInChunk,
        const common::ValueVector& values);

    uint64_t getEstimatedMemoryUsage() const {
        return residencyState == ResidencyState::ON_DISK ? 0 : data->getEstimatedMemoryUsage();
    }
    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<ColumnChunk> deserialize(common::Deserializer& deSer);

    void setMetadata(const ColumnChunkMetadata& metadata) const { data->setMetadata(metadata); }
    const ColumnChunkMetadata& getFlushedMetadata() const { return data->getFlushedMetadata(); }

    uint64_t getNumValues() const { return data->getNumValues(); }
    void setNumValues(const uint64_t numValues) const { data->setNumValues(numValues); }

    // Functions to access the in memory data.
    ColumnChunkData& getData() const { return *data; }
    const ColumnChunkData& getConstData() const { return *data; }
    std::unique_ptr<ColumnChunkData> moveData() { return std::move(data); }

    common::LogicalType& getDataType() { return dataType; }
    const common::LogicalType& getDataType() const { return dataType; }
    bool isCompressionEnabled() const { return enableCompression; }

    ResidencyState getResidencyState() const { return data->getResidencyState(); }
    bool hasUpdates() const { return updateInfo != nullptr; }

    // These functions should only work on in-memory and temporary column chunks.
    void resetToEmpty() { data->resetToEmpty(); }
    void setAllNull() { data->setAllNull(); }
    void resize(uint64_t newSize) { data->resize(newSize); }

private:
    ResidencyState residencyState;
    common::LogicalType dataType;
    bool enableCompression;
    std::unique_ptr<ColumnChunkData> data;
    // Update versions.
    std::unique_ptr<UpdateInfo> updateInfo;
};

} // namespace storage
} // namespace kuzu
