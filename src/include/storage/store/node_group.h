#pragma once

#include "catalog/catalog.h"
#include "processor/result/result_set.h"
#include "storage/store/column_chunk.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class NodeGroup {
public:
    explicit NodeGroup(catalog::TableSchema* schema, common::CopyDescription* copyDescription);

    inline void setNodeGroupIdx(uint64_t nodeGroupIdx_) { this->nodeGroupIdx = nodeGroupIdx_; }
    inline uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    inline common::offset_t getNumNodes() const { return numNodes; }
    inline ColumnChunk* getColumnChunk(common::property_id_t propertyID) {
        return chunks.contains(propertyID) ? chunks.at(propertyID).get() : nullptr;
    }
    inline catalog::TableSchema* getSchema() const { return schema; }
    inline void resetToEmpty() {
        numNodes = 0;
        nodeGroupIdx = UINT64_MAX;
        for (auto& [_, chunk] : chunks) {
            chunk->resetToEmpty();
        }
    }

    uint64_t append(processor::ResultSet* resultSet, std::vector<processor::DataPos> dataPoses,
        uint64_t numValuesToAppend);

    common::offset_t appendNodeGroup(NodeGroup* other, common::offset_t offsetInOtherNodeGroup);

private:
    uint64_t nodeGroupIdx;
    common::offset_t numNodes;
    std::unordered_map<common::property_id_t, std::unique_ptr<ColumnChunk>> chunks;
    catalog::TableSchema* schema;
    common::CopyDescription* copyDescription;
};

} // namespace storage
} // namespace kuzu
