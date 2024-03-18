#pragma once

#include "common/column_data_format.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class Column;

class NodeGroup {
public:
    NodeGroup(const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity);
    NodeGroup(const std::vector<std::unique_ptr<Column>>& columns, bool enableCompression);
    virtual ~NodeGroup() = default;

    inline uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    inline common::row_idx_t getNumRows() const { return numRows; }
    inline ColumnChunk* getColumnChunkUnsafe(common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return chunks[columnID].get();
    }
    inline const ColumnChunk& getColumnChunk(common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    inline bool isFull() const { return numRows == common::StorageConstants::NODE_GROUP_SIZE; }

    void resetToEmpty();
    void setAllNull();
    void setNumValues(common::offset_t numValues);
    void resizeChunks(uint64_t newSize);

    uint64_t append(const std::vector<common::ValueVector*>& columnVectors,
        common::DataChunkState* columnState, uint64_t numValuesToAppend);
    common::offset_t append(NodeGroup* other, common::offset_t offsetInOtherNodeGroup);
    void write(std::vector<std::unique_ptr<ColumnChunk>>& data, common::vector_idx_t offsetVector);

    void finalize(uint64_t nodeGroupIdx_);

    virtual inline void writeToColumnChunk(common::vector_idx_t chunkIdx,
        common::vector_idx_t vectorIdx, std::vector<std::unique_ptr<ColumnChunk>>& data,
        ColumnChunk& offsetChunk) {
        chunks[chunkIdx]->write(data[vectorIdx].get(), &offsetChunk, false /*isCSR*/);
    }

protected:
    std::vector<std::unique_ptr<ColumnChunk>> chunks;

private:
    uint64_t nodeGroupIdx;
    common::row_idx_t numRows;
};

struct CSRHeaderChunks {
    std::unique_ptr<ColumnChunk> offset;
    std::unique_ptr<ColumnChunk> length;

    CSRHeaderChunks() {}
    explicit CSRHeaderChunks(
        bool enableCompression, uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE);

    common::offset_t getStartCSROffset(common::offset_t nodeOffset) const;
    common::offset_t getEndCSROffset(common::offset_t nodeOffset) const;
    common::length_t getCSRLength(common::offset_t nodeOffset) const;

    bool sanityCheck() const;
    void copyFrom(const CSRHeaderChunks& other) const;
    void fillDefaultValues(common::offset_t newNumValues) const;
    inline void setNumValues(common::offset_t numValues) const {
        offset->setNumValues(numValues);
        length->setNumValues(numValues);
    }
};

class CSRNodeGroup : public NodeGroup {
public:
    CSRNodeGroup(const std::vector<common::LogicalType>& columnTypes, bool enableCompression);

    CSRHeaderChunks& getCSRHeader() { return csrHeaderChunks; }
    const CSRHeaderChunks& getCSRHeader() const { return csrHeaderChunks; }

    inline void writeToColumnChunk(common::vector_idx_t chunkIdx, common::vector_idx_t vectorIdx,
        std::vector<std::unique_ptr<ColumnChunk>>& data, ColumnChunk& offsetChunk) override {
        chunks[chunkIdx]->write(data[vectorIdx].get(), &offsetChunk, true /* isCSR */);
    }

private:
    CSRHeaderChunks csrHeaderChunks;
};

struct NodeGroupFactory {
    static inline std::unique_ptr<NodeGroup> createNodeGroup(common::ColumnDataFormat dataFormat,
        const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE) {
        return dataFormat == common::ColumnDataFormat::REGULAR ?
                   std::make_unique<NodeGroup>(columnTypes, enableCompression, capacity) :
                   std::make_unique<CSRNodeGroup>(columnTypes, enableCompression);
    }
};

} // namespace storage
} // namespace kuzu
