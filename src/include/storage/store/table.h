#pragma once

#include "common/enums/zone_map_check_result.h"
#include "storage/predicate/column_predicate.h"
#include "storage/stats/table_statistics_collection.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator
namespace storage {

enum class TableScanSource : uint8_t { COMMITTED = 0, UNCOMMITTED = 1, NONE = 3 };

struct TableScanState {
    common::ValueVector* nodeIDVector;
    std::vector<common::column_id_t> columnIDs;
    std::vector<common::ValueVector*> outputVectors;

    TableScanSource source = TableScanSource::NONE;
    std::unique_ptr<TableDataScanState> dataScanState;
    common::node_group_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;

    std::vector<ColumnPredicateSet> columnPredicateSets;
    common::ZoneMapCheckResult zoneMapResult = common::ZoneMapCheckResult::ALWAYS_SCAN;

    TableScanState(std::vector<common::column_id_t> columnIDs,
        std::vector<ColumnPredicateSet> columnPredicateSets)
        : nodeIDVector(nullptr), columnIDs{std::move(columnIDs)},
          columnPredicateSets{std::move(columnPredicateSets)} {}
    virtual ~TableScanState() = default;
    DELETE_COPY_AND_MOVE(TableScanState);

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TableScanState&, TARGET&>(*this);
    }
};

struct TableInsertState {
    const std::vector<common::ValueVector*>& propertyVectors;

    explicit TableInsertState(const std::vector<common::ValueVector*>& propertyVectors)
        : propertyVectors{propertyVectors} {}
    virtual ~TableInsertState() = default;
};

struct TableUpdateState {
    common::column_id_t columnID;
    const common::ValueVector& propertyVector;

    TableUpdateState(common::column_id_t columnID, const common::ValueVector& propertyVector)
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
    void updateNumTuplesByValue(uint64_t numTuples) const {
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
    virtual bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) = 0;

    virtual void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        evaluator::ExpressionEvaluator& defaultEvaluator) = 0;
    virtual void dropColumn(common::column_id_t columnID) = 0;

    virtual void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) = 0;
    // For metadata-only updates
    virtual void prepareCommit() = 0;
    virtual void prepareRollback(LocalTable* localTable) = 0;
    virtual void checkpointInMemory() = 0;
    virtual void rollbackInMemory() = 0;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<Table*, TARGET*>(this);
    }

protected:
    virtual bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) = 0;

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
