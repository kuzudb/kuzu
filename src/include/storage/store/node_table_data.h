#pragma once

#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

class LocalTableData;

class NodeTableData final : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        const std::vector<catalog::Property>& properties, TablesStatistics* tablesStatistics,
        bool enableCompression);

    // This interface is node table specific, as rel table requires also relDataDirection.
    inline virtual void initializeReadState(transaction::Transaction* /*transaction*/,
        std::vector<common::column_id_t> columnIDs, common::ValueVector* /*inNodeIDVector*/,
        TableReadState* readState) {
        readState->columnIDs = std::move(columnIDs);
    }
    void scan(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;
    void lookup(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    // These interfaces are node table specific, as rel table requires also relIDVector.
    // insert/update/delete_ keeps changes inside the local storage.
    void insert(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector);
    void delete_(transaction::Transaction* transaction, common::ValueVector* nodeIDVector);

    // Flush the nodeGroup to disk and update metadataDAs.
    void append(ChunkedNodeGroup* nodeGroup) override;

    void prepareLocalTableToCommit(
        transaction::Transaction* transaction, LocalTableData* localTable) override;

    inline common::node_group_idx_t getNumNodeGroups(
        transaction::Transaction* transaction) const override {
        return columns[0]->getNumNodeGroups(transaction);
    }

private:
    void append(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        LocalNodeGroup* localNodeGroup);
    void merge(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        LocalNodeGroup* nodeGroup);
};

} // namespace storage
} // namespace kuzu
