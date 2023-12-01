#pragma once

#include "catalog/catalog.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/store/table_data.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace storage {

class LocalTable;

class Table {
public:
    Table(catalog::TableSchema* tableSchema, TablesStatistics* tablesStatistics,
        MemoryManager* memoryManager, WAL* wal)
        : tableType{tableSchema->tableType},
          tablesStatistics{tablesStatistics}, tableID{tableSchema->tableID},
          memoryManager{memoryManager}, bufferManager{memoryManager->getBufferManager()}, wal{wal} {
    }
    virtual ~Table() = default;

    inline common::TableType getTableType() const { return tableType; }
    inline common::table_id_t getTableID() const { return tableID; }

    virtual void read(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) = 0;

    virtual void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) = 0;
    virtual void dropColumn(common::column_id_t columnID) = 0;

    virtual void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) = 0;
    virtual void prepareRollback(LocalTableData* localTable) = 0;
    virtual void checkpointInMemory() = 0;
    virtual void rollbackInMemory() = 0;

protected:
    common::TableType tableType;
    TablesStatistics* tablesStatistics;
    common::table_id_t tableID;
    MemoryManager* memoryManager;
    BufferManager* bufferManager;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
