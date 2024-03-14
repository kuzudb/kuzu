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

    virtual void append(ChunkedNodeGroup* nodeGroup) = 0;

    inline void dropColumn(common::column_id_t columnID) {
        columns.erase(columns.begin() + columnID);
    }
    void addColumn(transaction::Transaction* transaction, const std::string& colNamePrefix,
        InMemDiskArray<ColumnChunkMetadata>* metadataDA, const MetadataDAHInfo& metadataDahInfo,
        const catalog::Property& property, common::ValueVector* defaultValueVector,
        TablesStatistics* tableStats);

    inline common::vector_idx_t getNumColumns() const { return columns.size(); }
    inline Column* getColumn(common::column_id_t columnID) {
        KU_ASSERT(columnID < columns.size() && columnID != common::INVALID_COLUMN_ID);
        return columns[columnID].get();
    }

    virtual void prepareLocalTableToCommit(
        transaction::Transaction* transaction, LocalTableData* localTable) = 0;
    virtual void checkpointInMemory();
    virtual void rollbackInMemory();

    virtual common::node_group_idx_t getNumNodeGroups(
        transaction::Transaction* transaction) const = 0;

protected:
    TableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        bool enableCompression)
        : dataFH{dataFH}, metadataFH{metadataFH}, tableID{tableEntry->getTableID()},
          tableName{tableEntry->getName()}, bufferManager{bufferManager}, wal{wal},
          enableCompression{enableCompression} {}

protected:
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    common::table_id_t tableID;
    std::string tableName;
    BufferManager* bufferManager;
    WAL* wal;
    bool enableCompression;
    std::vector<std::unique_ptr<Column>> columns;
};

} // namespace storage
} // namespace kuzu
