#pragma once

#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

class LocalTableData;

class NodeTableData final : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, common::table_id_t tableID,
        BufferManager* bufferManager, WAL* wal, const std::vector<catalog::Property>& properties,
        TablesStatistics* tablesStatistics, bool enableCompression);

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

    // These two interfaces are node table specific, as rel table requires also relIDVector.
    void insert(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector);
    void delete_(transaction::Transaction* transaction, common::ValueVector* nodeIDVector);

    void append(NodeGroup* nodeGroup) override;

    void prepareLocalTableToCommit(
        transaction::Transaction* transaction, LocalTableData* localTable) override;

    inline common::node_group_idx_t getNumNodeGroups(
        transaction::Transaction* transaction) const override {
        return columns[0]->getNumNodeGroups(transaction);
    }
};

} // namespace storage
} // namespace kuzu
