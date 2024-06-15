#pragma once

#include "common/enums/zone_map_check_result.h"
#include "storage/predicate/column_predicate.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/store/node_group.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

enum class TableScanSource : uint8_t { COMMITTED = 0, UNCOMMITTED = 1, NONE = 3 };

struct TableScanState {
    common::table_id_t tableID;
    common::ValueVector* nodeIDVector;
    std::vector<common::ValueVector*> outputVectors;
    std::vector<common::column_id_t> columnIDs;
    std::vector<Column*> columns;
    std::vector<ChunkState> chunkStates;

    TableScanSource source = TableScanSource::NONE;
    NodeGroupScanState nodeGroupScanState;
    // TODO(Guodong): Should be removed after rework of rel tables.
    std::unique_ptr<TableDataScanState> dataScanState;
    common::node_group_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;

    std::vector<ColumnPredicateSet> columnPredicateSets;
    common::ZoneMapCheckResult zoneMapResult = common::ZoneMapCheckResult::ALWAYS_SCAN;

    TableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs,
        std::vector<Column*> columns, std::vector<ColumnPredicateSet> columnPredicateSets)
        : tableID{tableID}, nodeIDVector(nullptr), columnIDs{std::move(columnIDs)},
          columns{std::move(columns)}, columnPredicateSets{std::move(columnPredicateSets)} {
        chunkStates.resize(this->columns.size());
    }
    explicit TableScanState(const common::table_id_t tableID,
        std::vector<common::column_id_t> columnIDs, std::vector<Column*> columns)
        : tableID{tableID}, nodeIDVector(nullptr), columnIDs{std::move(columnIDs)},
          columns{std::move(columns)} {
        chunkStates.resize(this->columns.size());
    }
    virtual ~TableScanState() = default;
    DELETE_COPY_DEFAULT_MOVE(TableScanState);

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TableScanState&, TARGET&>(*this);
    }
    template<class TARGETT>
    const TARGETT& constCast() {
        return common::ku_dynamic_cast<const TableScanState&, const TARGETT&>(*this);
    }
};

struct TableInsertState {
    const std::vector<common::ValueVector*>& propertyVectors;

    explicit TableInsertState(const std::vector<common::ValueVector*>& propertyVectors)
        : propertyVectors{propertyVectors} {}
    virtual ~TableInsertState() = default;

    template<typename T>
    const T& constCast() {
        return common::ku_dynamic_cast<const TableInsertState&, const T&>(*this);
    }
    template<typename T>
    T& cast() {
        return common::ku_dynamic_cast<TableInsertState&, T&>(*this);
    }
};

struct TableUpdateState {
    common::column_id_t columnID;
    common::ValueVector& propertyVector;

    TableUpdateState(common::column_id_t columnID, common::ValueVector& propertyVector)
        : columnID{columnID}, propertyVector{propertyVector} {}
    virtual ~TableUpdateState() = default;
};

struct TableDeleteState {
    virtual ~TableDeleteState() = default;
};

class LocalTable;
class Table {
public:
    Table(const catalog::TableCatalogEntry* tableEntry, TablesStatistics* tablesStatistics,
        MemoryManager* memoryManager, WAL* wal)
        : tableType{tableEntry->getTableType()}, tableID{tableEntry->getTableID()},
          tableName{tableEntry->getName()}, tablesStatistics{tablesStatistics},
          memoryManager{memoryManager}, bufferManager{memoryManager->getBufferManager()}, wal{wal} {
    }
    virtual ~Table() = default;

    common::TableType getTableType() const { return tableType; }
    common::table_id_t getTableID() const { return tableID; }
    common::row_idx_t getNumTuples(transaction::Transaction* transaction) const {
        return tablesStatistics->getNumTuplesForTable(transaction, tableID);
    }
    void updateNumTuplesByValue(const uint64_t numTuples) const {
        tablesStatistics->updateNumTuplesByValue(tableID, numTuples);
    }

    virtual void initializeScanState(transaction::Transaction* transaction,
        TableScanState& readState) const = 0;
    bool scan(transaction::Transaction* transaction, TableScanState& scanState) {
        for (const auto& vector : scanState.outputVectors) {
            vector->resetAuxiliaryBuffer();
        }
        return scanInternal(transaction, scanState);
    }

    virtual void insert(transaction::Transaction* transaction, TableInsertState& insertState) = 0;
    virtual void update(transaction::Transaction* transaction, TableUpdateState& updateState) = 0;
    virtual void delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) = 0;

    virtual void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) = 0;
    virtual void dropColumn(common::column_id_t columnID) = 0;

    virtual void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) = 0;
    // For metadata-only updates
    virtual void prepareCommit() = 0;
    virtual void prepareRollback(LocalTable* localTable) = 0;
    virtual void checkpoint(common::Serializer& ser) = 0;
    virtual void rollbackInMemory() = 0;

    virtual uint64_t getEstimatedMemoryUsage() const = 0;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<Table&, TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<const Table&, const TARGET&>(*this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<Table*, TARGET*>(this);
    }

protected:
    virtual bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) = 0;
    virtual void checkpointInMemory() = 0;

protected:
    common::TableType tableType;
    common::table_id_t tableID;
    std::string tableName;
    TablesStatistics* tablesStatistics;
    MemoryManager* memoryManager;
    BufferManager* bufferManager;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
