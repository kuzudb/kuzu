#pragma once

#include "catalog/catalog.h"
#include "processor/result/result_set.h"
#include "storage/store/column_chunk.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class NodeTable;
class TableData;

class NodeGroup {
public:
    NodeGroup(const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
        bool enableCompression, uint64_t capacity);
    explicit NodeGroup(TableData* table);

    inline void setNodeGroupIdx(uint64_t nodeGroupIdx_) { this->nodeGroupIdx = nodeGroupIdx_; }
    inline uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    inline common::offset_t getNumNodes() const { return numNodes; }
    inline common::vector_idx_t getNumColumnChunks() { return chunks.size(); }
    inline ColumnChunk* getColumnChunk(common::column_id_t columnID) {
        assert(columnID < chunks.size());
        return chunks[columnID].get();
    }
    inline bool isFull() const { return numNodes == common::StorageConstants::NODE_GROUP_SIZE; }

    void resetToEmpty();

    uint64_t append(const std::vector<common::ValueVector*>& columnVectors,
        common::DataChunkState* columnState, uint64_t numValuesToAppend);

    common::offset_t append(NodeGroup* other, common::offset_t offsetInOtherNodeGroup);

    void append(common::DataChunk* dataChunk, common::vector_idx_t offsetVector);

private:
    uint64_t nodeGroupIdx;
    common::offset_t numNodes;
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
};

class CSRNodeGroup : public NodeGroup {
public:
    CSRNodeGroup(const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
        bool enableCompression)
        // By default, initialize all column chunks except for the csrOffsetChunk to empty, as they
        // should be resized after csr offset calculation (e.g., during CopyRel).
        : NodeGroup{columnTypes, enableCompression, 0 /* capacity */} {
        csrOffsetChunk =
            std::make_unique<ColumnChunk>(common::LogicalType{common::LogicalTypeID::INT64},
                common::StorageConstants::NODE_GROUP_SIZE, enableCompression);
    }

    inline ColumnChunk* getCSROffsetChunk() { return csrOffsetChunk.get(); }

private:
    std::unique_ptr<ColumnChunk> csrOffsetChunk;
};

struct NodeGroupFactory {
    static std::unique_ptr<NodeGroup> createNodeGroup(common::ColumnDataFormat dataFormat,
        const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
        bool enableCompression, uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE);
};

} // namespace storage
} // namespace kuzu
