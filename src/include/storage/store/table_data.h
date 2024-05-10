#pragma once

#include "storage/store/chunked_node_group.h"
#include "storage/store/column.h"

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
class Property;
} // namespace catalog

namespace storage {

struct TableDataReadState {
    TableDataReadState() = default;
    virtual ~TableDataReadState() = default;
    DELETE_COPY_DEFAULT_MOVE(TableDataReadState);

    std::vector<common::column_id_t> columnIDs;

    virtual bool hasMoreToRead(const transaction::Transaction* transaction) = 0;
};

class LocalTableData;
class TableData {
public:
    virtual ~TableData() = default;

    virtual void scan(transaction::Transaction* transaction, TableDataReadState& readState,
        common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) = 0;
    virtual void lookup(transaction::Transaction* transaction, TableDataReadState& readState,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) = 0;

    virtual void append(transaction::Transaction* transaction, ChunkedNodeGroup* nodeGroup) = 0;

    void dropColumn(common::column_id_t columnID) { columns.erase(columns.begin() + columnID); }
    void addColumn(transaction::Transaction* transaction, const std::string& colNamePrefix,
        DiskArray<ColumnChunkMetadata>* metadataDA, const MetadataDAHInfo& metadataDahInfo,
        const catalog::Property& property, common::ValueVector* defaultValueVector);

    common::vector_idx_t getNumColumns() const { return columns.size(); }
    Column* getColumn(common::column_id_t columnID) const {
        KU_ASSERT(columnID < columns.size() && columnID != common::INVALID_COLUMN_ID);
        return columns[columnID].get();
    }
    const std::vector<std::unique_ptr<Column>>& getColumns() const { return columns; }

    virtual void prepareLocalTableToCommit(transaction::Transaction* transaction,
        LocalTableData* localTable) = 0;
    virtual void checkpointInMemory();
    virtual void rollbackInMemory();
    virtual void prepareCommit();

    virtual common::node_group_idx_t getNumNodeGroups(
        const transaction::Transaction* transaction) const = 0;

protected:
    TableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        bool enableCompression);

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
