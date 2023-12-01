#pragma once

#include "storage/store/column.h"
#include "storage/store/node_group.h"

namespace kuzu {
namespace storage {

struct TableReadState {
    virtual ~TableReadState() = default;

    std::vector<common::column_id_t> columnIDs;
};

class LocalTableData;
class TableData {
public:
    virtual ~TableData() = default;

    virtual void scan(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) = 0;
    virtual void lookup(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) = 0;

    virtual void append(NodeGroup* nodeGroup) = 0;

    inline void dropColumn(common::column_id_t columnID) {
        columns.erase(columns.begin() + columnID);
    }
    void addColumn(transaction::Transaction* transaction,
        InMemDiskArray<ColumnChunkMetadata>* metadataDA, const MetadataDAHInfo& metadataDahInfo,
        const catalog::Property& property, common::ValueVector* defaultValueVector,
        TablesStatistics* tableStats);

    inline common::vector_idx_t getNumColumns() const { return columns.size(); }
    inline Column* getColumn(common::column_id_t columnID) {
        KU_ASSERT(columnID < columns.size());
        return columns[columnID].get();
    }
    inline common::node_group_idx_t getNumNodeGroups(transaction::Transaction* transaction) const {
        KU_ASSERT(!columns.empty());
        return columns[0]->getNumNodeGroups(transaction);
    }

    virtual void prepareLocalTableToCommit(
        transaction::Transaction* transaction, LocalTableData* localTable) = 0;
    virtual void checkpointInMemory();
    virtual void rollbackInMemory();

protected:
    TableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, common::table_id_t tableID,
        BufferManager* bufferManager, WAL* wal, bool enableCompression,
        common::ColumnDataFormat dataFormat)
        : dataFH{dataFH}, metadataFH{metadataFH}, tableID{tableID}, bufferManager{bufferManager},
          wal{wal}, enableCompression{enableCompression}, dataFormat{dataFormat} {}

protected:
    common::ColumnDataFormat dataFormat;
    std::vector<std::unique_ptr<Column>> columns;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    common::table_id_t tableID;
    BufferManager* bufferManager;
    WAL* wal;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
