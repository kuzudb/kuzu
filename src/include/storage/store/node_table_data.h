#pragma once

#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

class NodeTableData : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, common::table_id_t tableID,
        BufferManager* bufferManager, WAL* wal, const std::vector<catalog::Property*>& properties,
        TablesStatistics* tablesStatistics, bool enableCompression);

    inline virtual void initializeReadState(transaction::Transaction* /*transaction*/,
        std::vector<common::column_id_t> columnIDs, common::ValueVector* /*inNodeIDVector*/,
        TableReadState* readState) {
        readState->columnIDs = std::move(columnIDs);
    }
    void scan(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) final;
    void lookup(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) final;

    void append(NodeGroup* nodeGroup) final;
};

} // namespace storage
} // namespace kuzu
