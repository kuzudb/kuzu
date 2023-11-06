#pragma once

#include "common/column_data_format.h"
#include "common/data_chunk/data_chunk.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class Column;

class NodeGroup {
public:
    NodeGroup(const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
        bool enableCompression, bool needFinalize, uint64_t capacity);
    explicit NodeGroup(const std::vector<std::unique_ptr<Column>>& columns, bool enableCompression);
    virtual ~NodeGroup() = default;

    inline uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    inline common::offset_t getNumNodes() const { return numNodes; }
    inline ColumnChunk* getColumnChunk(common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return chunks[columnID].get();
    }
    inline bool isFull() const { return numNodes == common::StorageConstants::NODE_GROUP_SIZE; }

    void resetToEmpty();
    void setChunkToAllNull(common::vector_idx_t chunkIdx);
    void resizeChunks(uint64_t newSize);

    uint64_t append(const std::vector<common::ValueVector*>& columnVectors,
        common::DataChunkState* columnState, uint64_t numValuesToAppend);
    common::offset_t append(NodeGroup* other, common::offset_t offsetInOtherNodeGroup);
    void write(common::DataChunk* dataChunk, common::vector_idx_t offsetVector);

    void finalize(uint64_t nodeGroupIdx_);

private:
    uint64_t nodeGroupIdx;
    common::offset_t numNodes;
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
};

class CSRNodeGroup : public NodeGroup {
public:
    CSRNodeGroup(const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
        bool enableCompression, bool needFinalize)
        // By default, initialize all column chunks except for the csrOffsetChunk to empty, as they
        // should be resized after csr offset calculation (e.g., during CopyRel).
        : NodeGroup{columnTypes, enableCompression, needFinalize, 0 /* capacity */} {
        csrOffsetChunk = ColumnChunkFactory::createColumnChunk(
            common::LogicalType{common::LogicalTypeID::INT64}, enableCompression);
    }

    inline ColumnChunk* getCSROffsetChunk() { return csrOffsetChunk.get(); }

private:
    std::unique_ptr<ColumnChunk> csrOffsetChunk;
};

struct NodeGroupFactory {
    static std::unique_ptr<NodeGroup> createNodeGroup(common::ColumnDataFormat dataFormat,
        const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
        bool enableCompression, bool needFinalize = false,
        uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE);
};

} // namespace storage
} // namespace kuzu
