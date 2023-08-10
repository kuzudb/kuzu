#pragma once

#include "catalog/catalog.h"
#include "column_chunk.h"
#include "processor/result/result_set.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class NodeTable;

class NodeGroup {
public:
    explicit NodeGroup(
        catalog::TableSchema* schema, common::CopyDescription* copyDescription = nullptr);
    explicit NodeGroup(NodeTable* table);

    inline void setNodeGroupIdx(uint64_t nodeGroupIdx_) { this->nodeGroupIdx = nodeGroupIdx_; }
    inline uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    inline common::offset_t getNumNodes() const { return numNodes; }
    inline ColumnChunk* getColumnChunk(common::property_id_t propertyID) {
        return chunks.contains(propertyID) ? chunks.at(propertyID).get() : nullptr;
    }
    inline bool isFull() const { return numNodes == common::StorageConstants::NODE_GROUP_SIZE; }

    void resetToEmpty();

    uint64_t append(processor::ResultSet* resultSet, std::vector<processor::DataPos> dataPoses,
        uint64_t numValuesToAppend);

    common::offset_t append(NodeGroup* other, common::offset_t offsetInOtherNodeGroup);

private:
    uint64_t nodeGroupIdx;
    common::offset_t numNodes;
    std::unordered_map<common::property_id_t, std::unique_ptr<ColumnChunk>> chunks;
};

} // namespace storage
} // namespace kuzu
